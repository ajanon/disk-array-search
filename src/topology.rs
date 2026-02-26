use std::collections::BTreeMap;

/// Logical CPU ID to die/package group.
/// Groups CPU cores by their L3 cache domain (die), so that threads assigned
/// to the same group share an L3 cache and won't incur cross-die snoop traffic.
type DieId = u32;

/// Reads the topology of the machine from sysfs.
///
/// Returns a map from die ID to the sorted list of logical CPU IDs (cores)
/// belonging to that die. Uses `die_id` when available (Linux 5.4+, covers
/// AMD multi-CCD), falling back to `physical_package_id`.
pub fn cores_by_die() -> BTreeMap<DieId, Vec<usize>> {
    let mut groups: BTreeMap<DieId, Vec<usize>> = BTreeMap::new();

    let dir = match std::fs::read_dir("/sys/devices/system/cpu") {
        Ok(d) => d,
        Err(_) => return groups,
    };

    for entry in dir.filter_map(Result::ok) {
        let name = entry.file_name().to_string_lossy().to_string();
        let suffix = match name.strip_prefix("cpu") {
            Some(s) if !s.is_empty() && s.chars().all(|c| c.is_ascii_digit()) => s,
            _ => continue,
        };
        let cpu_id: usize = match suffix.parse() {
            Ok(id) => id,
            Err(_) => continue,
        };

        // Prefer die_id (covers AMD multi-CCD), fall back to physical_package_id
        let die_id = read_topology_attr(&name, "die_id")
            .or_else(|| read_topology_attr(&name, "physical_package_id"))
            .unwrap_or(0);

        groups.entry(die_id).or_default().push(cpu_id);
    }

    for cores in groups.values_mut() {
        cores.sort_unstable();
    }

    groups
}

fn read_topology_attr(cpu_name: &str, attr: &str) -> Option<u32> {
    let path = format!("/sys/devices/system/cpu/{cpu_name}/topology/{attr}");
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Builds an ordered list of `parallelism` core IDs for rayon thread pinning.
///
/// Threads are packed by die so that consecutive rayon thread indices share
/// L3 cache. With 2 dies and `parallelism=4`:
///   thread 0 → die 0 core 0
///   thread 1 → die 0 core 1
///   thread 2 → die 1 core 0
///   thread 3 → die 1 core 1
///
/// This pairs nicely with sequential block dispatch: rayon tends to give
/// lower-indexed threads the first blocks, so die 0's threads scan the first
/// half of the batch while die 1's threads scan the second half — each half
/// fitting in that die's L3.
pub fn pinning_list(parallelism: usize, groups: &BTreeMap<DieId, Vec<usize>>) -> Vec<usize> {
    if groups.is_empty() {
        return vec![];
    }

    let n_dies = groups.len();
    let threads_per_die = parallelism.div_ceil(n_dies);

    let mut result = Vec::with_capacity(parallelism);
    'outer: for die_cores in groups.values() {
        for i in 0..threads_per_die {
            if result.len() >= parallelism {
                break 'outer;
            }
            // If a die has fewer physical cores than threads_per_die, reuse
            // the last core (SMT sibling would be better, but this is safe).
            let core_id = *die_cores.get(i).or_else(|| die_cores.last()).unwrap();
            result.push(core_id);
        }
    }

    result
}

/// Computes a batch multiplier that sizes the total batch to approximately
/// `fill_fraction` of the total L3 cache, so the data scanned by each thread
/// in a batch fits within its die's cache.
///
/// Formula: batch_multiplier = (l3_total_bytes × fill_fraction) / (parallelism
/// × block_size)
pub fn auto_batch_multiplier(
    l3_total_bytes: usize,
    parallelism: usize,
    block_size: usize,
    fill_fraction: f64,
) -> usize {
    let target = (l3_total_bytes as f64 * fill_fraction) as usize;
    (target / (parallelism * block_size)).max(1)
}

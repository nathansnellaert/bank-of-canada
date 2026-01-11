"""Bank of Canada connector - DAG orchestration.

DAG structure:
- series_list: Fetch and transform series catalog (no deps)
- groups: Fetch and transform groups catalog (no deps)
- series_data: Incrementally fetch observations for all series (depends on series_list)
- datasets: Transform series data into wide-format datasets (depends on series_data)
"""
from subsets_utils import DAG, validate_environment
from nodes import series_list, groups, series_data, datasets


workflow = DAG({
    series_list.run: [],              # Fetch and upload series list
    groups.run: [],                   # Fetch and upload groups (parallel with series_list)
    series_data.run: [series_list.run],  # Fetch series observations (needs series list)
    datasets.run: [series_data.run],     # Transform to wide datasets (needs series data)
})


def main():
    validate_environment()
    workflow.run()
    workflow.save_state()


if __name__ == "__main__":
    main()

#! /usr/bin/env bash
results_file="`date --utc +"%Y-%m-%d"`-results.csv"
rm -f $results_file
echo "Results file: $results_file"
echo "Running benchmarks..."
print_header_row="--results-csv-print-header-row"
for rows in 1 2 4 8 16; do
    for schema in placement; do
        for placement_strategy in pack spread random random-pack random-spread; do
            for filter_strategy in db python; do
                for partition_strategy in none modulo; do
                    echo -n "  $schema | $placement_strategy | $filter_strategy | $partition_strategy | $rows rows | 1 worker ... "
                    # We do a serial run, saving the instance requests
                    # to an output file that we then read in to perform
                    # the same scenario tests for the multi-worker variants
                    python benchmark.py --workers=1 \
                        --quiet --results-format=csv $print_header_row \
                        --rows=$rows \
                        --out-requests-file=requests.yaml \
                        --schema=$schema \
                        --filter-strategy=$filter_strategy \
                        --placement-strategy=$placement_strategy \
                        --partition-strategy=$partition_strategy \
                        --results-file=$results_file
                    echo "OK"
                    print_header_row=""
                    for workers in 2 4 8; do
                        echo -n "  $schema | $placement_strategy | $filter_strategy | $partition_strategy | $rows rows | $workers workers ... "
                        python benchmark.py --workers=$workers \
                            --quiet --results-format=csv $print_header_row \
                            --rows=$rows \
                            --in-requests-file=requests.yaml \
                            --schema=$schema \
                            --filter-strategy=$filter_strategy \
                            --placement-strategy=$placement_strategy \
                            --partition-strategy=$partition_strategy \
                            --results-file=$results_file
                        echo "OK"
                    done
                done
            done
        done
    done
done

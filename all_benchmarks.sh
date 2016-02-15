#! /usr/bin/env bash
for rows in 1 2 4 8; do
    for schema in placement; do
        for filter_strategy in db python; do
            for placement_strategy in pack spread random; do
                for partition_strategy in none modulo; do
                    # We do a serial run, saving the instance requests
                    # to an output file that we then read in to perform
                    # the same scenario tests for the multi-worker variants
                    python benchmark.py --workers=1 \
                        --rows=$rows \
                        --out-requests-file=requests.yaml \
                        --schema=$schema \
                        --filter-strategy=$filter_strategy \
                        --placement-strategy=$placement_strategy \
                        --partition-strategy=$partition_strategy \
                        --results-file=results.txt
                    for workers in 2 4 8; do
                        python benchmark.py --workers=$workers \
                            --rows=$rows \
                            --in-requests-file=requests.yaml \
                            --schema=$schema \
                            --filter-strategy=$filter_strategy \
                            --placement-strategy=$placement_strategy \
                            --partition-strategy=$partition_strategy \
                            --results-file=results.txt
                    done
                done
            done
        done
    done
done

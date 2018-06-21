DATE=$(date +%Y%m%d-%H%M%S)
for number in {1,3,5,7,9,11}
do
    echo "Starting benchmark for ${number} minutes..."
    python3 benchmark.py all_random --repetitions 20 --processes 120 --seed 5914 --duration ${number} > results/all_random_results_${DATE}_${number}
done


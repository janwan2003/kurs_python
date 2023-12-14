#!/bin/bash

# plik wyjściowy
output_file="wyniki_testow.txt"

# wyczyść poprzednie wyniki
echo "" > $output_file

n_values=(100000 500000 1000000 2000000)
k_values=(1 2 3)

for n in "${n_values[@]}"
do
    for k in "${k_values[@]}"
    do
        echo "Testowanie dla n=$n, k=$k" | tee -a $output_file

        echo "ulamek_bez_slots.py" | tee -a $output_file
        time_bez_slots=$(/usr/bin/time -v python3 ulamek_bez_slots.py $n $k 2>&1 1>/dev/null | grep -E "Elapsed|Max")
        echo "$time_bez_slots" | tee -a $output_file

        echo "ulamek_ze_slots.py" | tee -a $output_file
        time_ze_slots=$(/usr/bin/time -v python3 ulamek_ze_slots.py $n $k 2>&1 1>/dev/null | grep -E "Elapsed|Max")
        echo "$time_ze_slots" | tee -a $output_file

        echo "" | tee -a $output_file
    done
done

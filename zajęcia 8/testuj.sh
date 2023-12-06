#!/bin/bash

echo "Testowanie ulamek_bez_slots.py"
time_bez_slots=$(/usr/bin/time -v python3 ulamek_bez_slots.py 2000000 2 2>&1 1>/dev/null | grep -E "Elapsed|Max")
echo "$time_bez_slots"

echo "Testowanie ulamek_ze_slots.py"
time_ze_slots=$(/usr/bin/time -v python3 ulamek_ze_slots.py 2000000 2 2>&1 1>/dev/null | grep -E "Elapsed|Max")
echo "$time_ze_slots"

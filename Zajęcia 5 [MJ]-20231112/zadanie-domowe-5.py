import json
import sys
from functools import reduce


def clear_output(cell):
    if cell["cell_type"] == "code":
        cell["outputs"] = []
    return cell


def clear_code_after_exercise(acc, cell):
    if acc["last_was_exercise"] and cell["cell_type"] == "code":
        cell["source"] = []
    if cell["cell_type"] == "markdown" and cell["source"]:
        # Zakładamy, że pierwsza linia może być podzielona na wiele elementów listy
        first_line = "".join(cell["source"]).strip().split("\n")[0]
        acc["last_was_exercise"] = first_line.startswith("# Ćwiczenie")
    else:
        acc["last_was_exercise"] = False

    acc["cells"].append(cell)
    return acc


def process_notebook(input_file, output_file):
    with open(input_file, "r") as file:
        data = json.load(file)

    # Usuwamy outputy
    data["cells"] = list(map(clear_output, data["cells"]))

    # Usuwamy kod z odpowiednich komórek
    data["cells"] = reduce(
        clear_code_after_exercise,
        data["cells"],
        {"cells": [], "last_was_exercise": False},
    )["cells"]

    with open(output_file, "w") as file:
        json.dump(data, file, indent=4)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Użycie: python script.py plik_wejściowy.ipynb")
        sys.exit(1)

    input_filename = sys.argv[1]
    output_filename = input_filename.rsplit(".", 1)[0] + ".czysty.ipynb"
    process_notebook(input_filename, output_filename)

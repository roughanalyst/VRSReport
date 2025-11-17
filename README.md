# VRSReport

This repository now contains a command-line utility that replicates the
original Google Colab processing workflow locally so you can run it from
VS Code or any other environment.

## Requirements

- Python 3.10+
- Packages: `pandas`, `numpy`, and `xlsxwriter` (installable with
  `pip install -r requirements.txt` after generating the file yourself or
  by installing the packages directly).

## Usage

```
python process_vrs_report.py \
  --main /path/to/main_workbook.xlsx \
  --lot-master /path/to/lot_master.xlsx \
  --output processed_data_summary.xlsx
```

The script reads the required sheets from the supplied workbooks,
performs the same calculations as the Colab notebook (including damage
processing, sales calculations, R-EMD indicators, and the NNS summary),
and writes all intermediate tables to the specified output workbook.

All sheet names are matched case-insensitively and ignoring special
characters, mirroring the flexible matching behaviour of the notebook.

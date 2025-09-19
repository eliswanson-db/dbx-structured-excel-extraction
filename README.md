# Databricks Structured Excel Extraction

Convert Excel files to PDF format using Databricks structured streaming for downstream OCR and document processing.

## Overview

This solution processes Microsoft Excel files by:
1. Reading Excel files from a Databricks volume using structured streaming
2. Converting specific cell ranges to HTML tables
3. Generating PDF documents from the HTML content
4. Storing results in Databricks volumes for further processing

**Use Case**: R&D and experiment files with complex structured content, multiple tables, or variable formats that are difficult to parse directly. Converting to PDF enables consistent OCR processing and information extraction without custom parsing logic for each file variant.

https://docs.databricks.com/aws/en/machine-learning/reference-solutions/images-etl-inference#workflow-for-image-model-inferencing

Why not ingest binaries to a Delta table? The reasoning is because these files are likely intended for PDF processing by AgentBricks, so we want to write them out to start.

## Setup

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Source and destination volumes configured
- Python packages: `openpyxl==3.1.5`, `xhtml2pdf==0.2.17`

### Installation
1. Import `excel2pdf.py` into your Databricks workspace
2. Configure the required parameters using the notebook widgets
3. Run the notebook to start processing

### Configuration Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `catalog_name` | Unity Catalog name | `my_catalog` |
| `source_schema` | Schema containing source volume | `raw_data` |
| `source_volume` | Volume with Excel files | `excel_files` |
| `dest_schema` | Schema for destination volume | `processed_data` |
| `dest_volume` | Volume for PDF output | `pdf_files` |
| `dest_subfolder` | Subfolder for organization | `converted_pdfs` |
| `dest_metadata_table` | Table for processing metadata | `conversion_log` |
| `worksheet_name` | Excel worksheet to process | `Database` |

## Usage

1. **Setup volumes and permissions**:
Of course, the source schema and volume are only needed if the landing zone doesn't already exist.

   ```sql
   CREATE SCHEMA IF NOT EXISTS my_catalog.raw_data;
   CREATE VOLUME IF NOT EXISTS my_catalog.raw_data.excel_files;
   CREATE SCHEMA IF NOT EXISTS my_catalog.processed_data;
   CREATE VOLUME IF NOT EXISTS my_catalog.processed_data.pdf_files;
   ```

2. **Upload Excel files** to the source volume. If the source volume already exists you can skip this step.

3. **Configure and run** the notebook with your parameters

4. **Monitor processing** through the metadata table

## Architecture

- **Streaming**: Uses Databricks Auto Loader for incremental file processing
- **Scalability**: Pandas UDF enables distributed processing across cluster nodes
- **Error Handling**: Failed conversions are logged with null destination paths
- **Checkpointing**: Ensures exactly-once processing and recovery

## File Structure

The solution expects Excel files with a configurable worksheet (default: "Database") containing:
- Basic information in range A1:B18
- Main data in range C8:I50

This can be modified easily in the html building.

Set the `worksheet_name` parameter to target different worksheets. Modify the `xlsm_to_html()` function to adjust cell ranges for different Excel structures.

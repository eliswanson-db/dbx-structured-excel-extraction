# dbx-structured-excel-extraction

Purpose is to allow processing Microsoft Excel files to html tables and then to PDFs, to allow OCR + ingestion using AgentBricks in Databricks. 

Q. Why would I want to do that?
A. If you have R&D or other experiment files that have multiple tables, possibly images, or other structured content in a sheet, it can be very difficult to accurately parse. This tool will create PDFs with some basic parameters that can be run through OCR, and information pulled out. This can be a lot less work, and simpler than writing lots of parsing code, especially if your structured files change somewhat from sample to sample.

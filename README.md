# Alto Analyser

**Alto Analyser** is a Python-based application designed
to efficiently run analysis of [**19th Century Digitised Books Collection**](https://bl.iro.bl.uk/collections/b7fd2482-debd-4495-9494-72aa2ead00bb) 
from **British Library** in [**Analyzed Layout and Text Object(alto)**](https://en.wikipedia.org/wiki/Analyzed_Layout_and_Text_Object) format .
This project is built to be easy to configure, run, and extend for your specific needs.

## Features

- **Efficient Archive Analysis**: Perform analysis directly on decade-long Alto archives without the need for unpacking.
- **Token Filtering**: Filter tokens based on their OCR confidence levels to ensure accurate results.
- **White-List Driven Analysis**: Analyze token neighbors exclusively for user-specified tokens, ensuring focused and relevant insights.
- **Configurable Filtering**:
  - Define a custom list of words to exclude from the analysis.
  - Adjust token filtering thresholds based on OCR confidence.
- **Comprehensive Token Metrics**:
  - Count the occurrence of all tokens.
  - Calculate token frequencies and probabilities.
- **N-Window Neighbor Analysis**:
  - Identify and analyze the most probable neighboring tokens within a configurable window size.
---

## Table of Contents

- [General Description](#general-description)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Application](#running-the-application)
- [Performance](#performance)
- [License](#license)

---

## General Description

`Alto Analyser` is a tool developed to run analysis on digitalized books in alto format.  
It supports input in .zip format, output in .sqlite.  

---

## Installation

To get started with `Alto Analyser`, follow these steps:

1. Install python
   1. Check if Python is already installed:
      
      Open a terminal and run:
      ```bash
      python3 --version
      ```
      If Python is installed, you'll see the version number. If not, proceed with the steps below.
   
   2. Install Python via Homebrew (recommended):
   
      ```bash
      brew install python
      ```
      
   3. Install via Python installer:

      Go to the official Python website: https://www.python.org/downloads/

      Download the latest Python release for your system(macos, windows, linux).
   
      Follow official instruction to install python


2. Clone the repository:
   ```bash
   git clone https://github.com/akriuchk/alto-analyzer.git
   ```

3. Navigate to the project directory:

   ```bash
   cd alto
   ```

4. Create and activate a virtual environment (optional but recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate # For Linux/Mac
   venv\Scripts\activate # For Windows
   ```

5. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
    

## Configuration

Before running **Alto Analyser**, you may need to configure it based on your needs. Configuration options are
defined in the [config.py](src/config.py).

Open the configuration file and update settings:

    self.archive_path = 'data/in/1820_1829.zip' # you may need to create 'data' and 'in' folders or put alto archive wherever you want
    self.decade = '1820_1829' # parameter used for output
    self.windows = {3, 5} # list of windows
    self.words_for_analysis = {'when', 'under', 'kind'} # list of words for analysis. Freely use up to 100 words, more not tested
    ... 
    and many more, including parallelization, cache, memory usage tweaks 

Save and close the file.

## Running the Application

To run Alto:

Ensure your dependencies are installed and configurations are set.
Start the application using:

```bash
python src/main.py
```

Monitor the output for any logs or errors.

Result will be available in `self.output_path` folder, `self.decade.sqlite` db file.

## Performance
With default settings, on Macbook M1Pro 10gb archive analysed in 1:20hr, ~10k lines/sec
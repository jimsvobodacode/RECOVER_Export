<p align="center">

  <h2 align="center">RECOVER Export</h3>

  <p align="center">

  </p>
</p>


<!-- ABOUT THE PROJECT -->
## About The Project

The RECOVER Export project is to be used in support of the [RECOVER Project](https://nyc-cdrn.atlassian.net/wiki/home)

The export project will extract data from your PCORNET Common Data Model (CDM) database for the given patient cohort and save the data as delimited text files, per the RECOVER requirements.


### Built With

* Python 3.9.X 
* Uses package pyodbc
* Visual Studio Code


<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple steps.  Process has been tested on Windows 10 and 11.

### Prerequisites

If not done already, download and install the current version of Python from [python.org](https://www.python.org/).


### Installation

1. Clone the repo
   ```
   git clone https://github.com/jimsvobodacode/RECOVER_Export.git
   ```
2. Install pip package
   ```
   pip install pyodbc
   ```


<!-- USAGE EXAMPLES -->
## Usage

1. Modify the config.ini file [mssql] section to point to your CDM database
2. Modify the config.ini file [general] section to contain your site values
2. Open Powershell in Windows
3. Navigate to your project directory (ex. ```cd c:\myproject...```)
4. Run ```python app.py```
5. When done there will be a file named [site name]\_PCORNET\_CDM\_DD\_MM\_YY.tar
6. Upload the .tar file to your AWS Bucket using the AWS CLI

<!-- CONTRIBUTING -->
## Contributing

I'm only accepting bug fixes at this time.




<!-- CONTACT -->
## Contact

Jim Svoboda - jim.svoboda@unmc.edu

Feel free to contact me with questions or bug fixes.


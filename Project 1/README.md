## Compilation and Execution Instructions

The following commands can be executed in either Git Bash or the Command Prompt terminal.
Ensure that the GCC compiler is installed and properly configured in your system’s environment path.

Note:
The program was originally compiled using GCC on a local machine.
If you are using VS Code, you may open the integrated terminal (Git Bash or Command Prompt) and run the same commands below.

### Command Lines for Task 1
1. Compilation
Compile all the source files using the GCC compiler:

``` gcc -std=c11 -O2 -Iheader src/*.c -o project_c ``` 

This command generates the executable file project_c.


2. Loading Data
Run the load command to read data from games.txt, encode each record, and store them sequentially into the binary heap file data.db, using a buffer size of 64:

``` ./project_c load games.txt data.db --buf 64 ``` 

This step creates the database file and stores all records into persistent storage.


3. Viewing Statistics
Run the stats command to display key statistics of the heap file, such as record size, records per block, total blocks, and I/O counts:

``` ./project_c stats data.db --buf 64 ``` 

This command only reads data from the heap file to compute and display statistics.


4. Scanning Records
Use the scan command to view the first few records stored in data.db:

``` ./project_c scan data.db --buf 64 --limit 10 ``` 


This confirms that the storage component successfully loads, stores, and retrieves data from disk using the buffer pool for optimized access.



### Command Lines for Task 2

1. Build the B+ Tree Index
Construct the B+ tree file (`btree.db`) from the heap file created in Task 1

``` ./project_c build_bplus data.db ```

This command scans every record in `data.db`, sorts the index keys, and writes the resulting B+ tree structure to `btree.db`. You should run it after the heap file has been populated to ensure Task 2 artifacts are up to date.

### Command Lines for Task 3

1. Delete Records above min_key

 ``` ./project_c delete_bplus data.db 0.9 ``` 
 
 This command adjusts the value of min_key and it will delete all records above it, and it will also run comparison tests against a brute-force linear scan search.
 The min_key can be adjusted to any key value for other range deletion commands.

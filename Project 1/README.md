how to run

1. cd to the folder

run below lines:
gcc -std=c11 -O2 -Iheader src/*.c -o project_c
./project_c load games.txt data.db --buf 64
./project_c stats data.db --buf 64
./project_c scan data.db --buf 64 --limit 10

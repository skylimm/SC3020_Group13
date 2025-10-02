how to run for task 1 & 2
i ran my codes in vscode <br />
not sure if you guys have the compliers in vscode but i originally have gcc installed in my computer and works for me.
yall can try to run below command lines in the gitbash terminal 

1. cd to the folder

2. run below lines:
`gcc -std=c11 -O2 -Iheader src/*.c -o project_c`<br />
`./project_c load games.txt data.db --buf 64`<br />
`./project_c stats data.db --buf 64`<br />
`./project_c scan data.db --buf 64 --limit 10`

`./project_c build_bplus  data.db` : this is for task 2. sometimes might give an error but just rerun.

`./project_c delete_bplus data.db 0.9` : for task 3. you can adjust the value of min_key and it will delete all records above it.
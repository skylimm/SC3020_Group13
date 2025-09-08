how to run for task 1
i ran my codes in vscode <br />
not sure if you guys have the compliers in vscode but i originally have gcc installed in my computer and works for me.
yall can try to run below command lines in the gitbash terminal 

1. cd to the folder

2. run below lines:
`gcc -std=c11 -O2 -Iheader src/*.c -o project_c`<br />
`./project_c load games.txt data.db --buf 64`<br />
`./project_c stats data.db --buf 64`<br />
`./project_c scan data.db --buf 64 --limit 10`

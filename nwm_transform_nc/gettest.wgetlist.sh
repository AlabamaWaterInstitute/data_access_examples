# tip from here: https://askubuntu.com/questions/214018/how-to-make-wget-faster-or-multithreading
# https://stackoverflow.com/questions/7577615/parallel-wget-in-bash
# and here: https://github.com/axel-download-accelerator/axel/issues/168
cat gettest.wgetlist.txt | xargs -n 1 -P 8 wget -P data -c
cat gettest.wgetlist_medium.txt | xargs -n 1 -P 8 wget -P data -c

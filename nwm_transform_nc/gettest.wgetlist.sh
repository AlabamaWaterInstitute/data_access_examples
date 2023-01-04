# tip from here: https://askubuntu.com/questions/214018/how-to-make-wget-faster-or-multithreading
# and here: https://github.com/axel-download-accelerator/axel/issues/168
cat gettest.wgetlist.txt | xargs wget -P data -c
cat gettest.wgetlist_medium.txt | xargs wget -P data -c

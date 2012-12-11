# echo "Personal"

scan_path=~/Personal
scan_name=Personal_fast
# ruby dirscan.rb sq $scan_path tmp/$scan_name.dirscan
# ruby dirscan.rb u tmp/$scan_name.dirscan tmp/$scan_name.json
# ruby dirscan.rb a tmp/$scan_name.dirscan tmp/$scan_name.analysis
ruby dirscan.rb ar tmp/$scan_name.analysis tmp/$scan_name.report



# echo "Personal/Chameleon"

# scan_path=~/Personal/Chameleon
# scan_name=Chameleon
# # scan-quick
# ruby dirscan.rb sq $scan_path tmp/$scan_name.dirscan
# # unpack
# ruby dirscan.rb u tmp/$scan_name.dirscan tmp/$scan_name.json
# # analyze
# ruby dirscan.rb a tmp/$scan_name.dirscan tmp/$scan_name.analysis

# # echo "Calculating content_size using find + awk"
# find $scan_path -type file -ls | awk '{total += $7} END {print total}'

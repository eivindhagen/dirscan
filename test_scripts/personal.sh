# dir=~/Personal
# ruby dirscan.rb s test_data/$dir/ tmp/$dir.dirscan
# ruby dirscan.rb u  tmp/$dir.dirscan tmp/$dir.json

scan_path=~/Personal/Chameleon
scan_name=Chameleon
ruby dirscan.rb sq $scan_path tmp/$scan_name.dirscan
ruby dirscan.rb u tmp/$scan_name.dirscan tmp/$scan_name.json

dir=empty
ruby dirscan.rb s test_data/$dir/ tmp/$dir.dirscan
ruby dirscan.rb u  tmp/$dir.dirscan tmp/$dir.json

dir=one_file
ruby dirscan.rb s test_data/$dir/ tmp/$dir.dirscan
ruby dirscan.rb u  tmp/$dir.dirscan tmp/$dir.json

dir=nested1
ruby dirscan.rb s test_data/$dir/ tmp/$dir.dirscan
ruby dirscan.rb u  tmp/$dir.dirscan tmp/$dir.json

dir=nested1_clone
ruby dirscan.rb s test_data/$dir/ tmp/$dir.dirscan
ruby dirscan.rb u  tmp/$dir.dirscan tmp/$dir.json

dir=nested1_with_extra_dir
ruby dirscan.rb s test_data/$dir/ tmp/$dir.dirscan
ruby dirscan.rb u  tmp/$dir.dirscan tmp/$dir.json


dirscan
=======

Directory scanner.

usage
=====

To scan a directory:
```
ruby dirscan.rb s path/to/dir
```
This will create a .dirscan_{number} file in the scan root dir

To verify a previous scan:
```
ruby dirscan.rb v path/to/dir/.dirscan_{number}
```
This will compare the scan info to the actual files, and reports any differences

To unpack a scanfile into a human readable format:
```
ruby dirscan.rb u path/to/dir/.dirscan_{number} path/to/file.json
```
This will convert the dirscan-file into a text file where each record is a JSON dump


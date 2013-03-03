# readme.rb

# Store sparse tree of sha256-sums for a set of files, and allow loading & merging of multiple such files (caching)
  # This may need to also store the last-modified time, so the sha256-sum can be expired.
# Build test scenarios with symlinks, in order to test the symlink content_hash

#
# sequential scan, active_dirs storage & roll-up
#
# ----- ----- ----- ------------------------- --------------
#                                             active_dirs
# dir   file  I/F   active_dirs               special action
# ----- ----- ----- ------------------------- --------------
# a1/         I     {'a1'}
# a1/b1/      I     {'a1', 'a1/b1'}
#       x
#       y
#       z
# a1/b1/      F     {'a1', 'a1/b1'}           remove 'a1/b1/*'
# a1/b2/      I     {'a1', 'a1/b1', 'a1/b2'}
#       x
#       y
#       z
# a1/b2/      F     {'a1', 'a1/b1', 'a1/b2'}  remove 'a1/b2/*'
# a1/         F     {'a1'}                    remove 'a1/*'
#

# Invent Help: 1 800 335 9236

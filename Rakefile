# Dir.glob('tasks/*.rake').each { |r| puts "importing #{r}"; import r }

task :default => [:test]

require 'rake/testtask'

Rake::TestTask.new do |t|
  t.libs << "test"
  t.test_files = FileList['test/*_test.rb']
  t.verbose = true
end

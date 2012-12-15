# A sample Guardfile
# More info at https://github.com/guard/guard#readme

guard :bundler do
  watch('Gemfile')
end

guard :test do
	watch(%r{^lib/(.+)\.rb$})	{ |m| "test/#{m[1]}_test.rb" }  
	watch(%r{^test/(.+)\.rb$})
	watch(%r{(.+)\.rb$}) {"test/ruby_sources_test.rb"}
	watch("test_data/one_file/file1.txt") {"test/pathinfo.rb"}
end

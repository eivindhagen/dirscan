# A sample Guardfile
# More info at https://github.com/guard/guard#readme

guard :bundler do
  watch('Gemfile')
end

guard :test do
	watch(%r{^lib/(.+)\.rb$})	{ |m| "test/#{m[1]}_test.rb" }  
	watch(%r{^test/(.+)\.rb$})
end

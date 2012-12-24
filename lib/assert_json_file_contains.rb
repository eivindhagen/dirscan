def assert_json_file_contains(expected, file_path, message = nil)
  assert(File.exist?(file_path), "File #{file_path} should exist")

  actual = File.open(file_path){|f| JSON.load(f)}
  assert_equal(expected.to_json, actual.to_json, message)
end


def assert_file_contains(expected, file_path, message = nil)
  assert(File.exist?(file_path), "File #{file_path} should exist")

  expected_size = expected.size
  actual_size = File.size(file_path)
  assert_equal(expected_size, actual_size, "File #{file_path} should be #{expected_size} bytes in size.")
  File.open(file_path) do |f|
    actual = f.read
    assert_equal(expected, actual, "File #{file_path} should contain the expected data.")
  end
end

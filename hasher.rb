class Hasher
	# replaces each <entry> found in the hash_template with it's corresponding value from the info hash
	def initialize(template, info)
		keys = template.split(HASH_SRC_SPLIT)
		values = keys.map{|k| info[k]}
		@source = values.join(HASH_SRC_JOIN)
		@hash = string_sha256(source)
		return self
	end

	def source
		return @source
	end

	def hash
		return @hash
	end
end

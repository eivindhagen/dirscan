class Hash
  # convert a hash so that all the keys are symbols
  def symbolize_keys
    t = self.dup
    self.clear
    t.each_pair{|k, v| self[k.to_sym] = v}
    self
  end
end

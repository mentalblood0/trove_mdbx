require "benchmark"

require "./src/trove_mdbx.cr"
require "./spec/common.cr"

chest = Trove::Chest.from_yaml <<-YAML
env:
  path: /tmp/trove_mdbx
  mode: 0o664
  flags:
    - MDBX_NOSUBDIR
    - MDBX_LIFORECLAIM
  mode: 0o664
  db_flags:
    d:
      - MDBX_DB_DEFAULTS
      - MDBX_CREATE
    i:
      - MDBX_DB_DEFAULTS
      - MDBX_CREATE
    u:
      - MDBX_DB_DEFAULTS
      - MDBX_CREATE
    o:
      - MDBX_DB_DEFAULTS
      - MDBX_CREATE
YAML

cs = JSON.parse COMPLEX_STRUCTURE.to_json
k = "level1.level2.level3.1.metadata.level4.level5.level6.note"
v = "This is six levels deep"

Benchmark.ips do |b|
  b.report "set+delete" do
    chest.transaction { |tx| tx.delete tx << cs }
  end
end

i = Bytes.new 0
chest.transaction { |tx| i = tx << cs }
Benchmark.ips do |b|
  b.report "has key" do
    chest.transaction { |tx| raise "Can not get" unless tx.has_key? i, k }
  end
  b.report "has key (only simple)" do
    chest.transaction { |tx| raise "Can not get" unless tx.has_key! i, k }
  end
  b.report "get full" do
    chest.transaction { |tx| raise "Can not get" if tx.get(i) != cs }
  end
  b.report "get field" do
    chest.transaction { |tx| raise "Can not get" if tx.get(i, k) != v }
  end
  b.report "get field (only simple)" do
    chest.transaction { |tx| raise "Can not get" if tx.get!(i, k) != v }
  end
end
Benchmark.ips do |b|
  n = 10**4 - 1
  (1..n).each { chest.transaction { |tx| tx << cs } }
  b.report "get one oid from index (unique)" do
    chest.transaction { |tx| tx.unique k, v }
  end
  b.report "get one oid from index" do
    chest.transaction { |tx| tx.where(k, v) { |ii| break } }
  end
  b.report "get #{n + 1} oids from index" do
    g = 0
    chest.transaction { |tx| tx.where(k, v) { |ii| g += 1 } }
    raise "#{g} != #{n + 1}" if g != n + 1
  end
end

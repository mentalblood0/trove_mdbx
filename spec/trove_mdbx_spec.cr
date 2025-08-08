require "spec"

require "../src/trove_mdbx"
require "./common.cr"

describe Trove do
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
      o:
        - MDBX_DB_DEFAULTS
        - MDBX_CREATE
  YAML

  it "example" do
    parsed = JSON.parse %({
                            "dict": {
                              "hello": ["number", 42, -4.2, 0.0],
                              "boolean": false
                            },
                            "null": null,
                            "array": [1, ["two", false], [null]]
                          })

    oid = Bytes.new 0
    chest.transaction do |tx|
      oid = tx << parsed
      tx.get(oid).should eq parsed
      tx.get(oid, "dict").should eq parsed["dict"]
      tx.get(oid, "dict.hello").should eq parsed["dict"]["hello"]
      tx.get(oid, "dict.boolean").should eq parsed["dict"]["boolean"]

      #   # get! is faster than get, but expects simple value
      #   # because under the hood all values in trove are simple
      #   # and get! just gets value by key, without range scan

      tx.get!(oid, "dict.boolean").should eq parsed["dict"]["boolean"]
      tx.get!(oid, "dict").should eq nil
      tx.get!(oid, "dict.hello.0").should eq parsed["dict"]["hello"][0]
      tx.get!(oid, "dict.hello.1").should eq parsed["dict"]["hello"][1]
      tx.get!(oid, "dict.hello.2").should eq parsed["dict"]["hello"][2]
      tx.get!(oid, "dict.hello.3").should eq parsed["dict"]["hello"][3]
      tx.get!(oid, "null").should eq nil
      tx.get!(oid, "nonexistent.key").should eq nil
      tx.get(oid, "array").should eq parsed["array"]
      tx.get!(oid, "array.0").should eq parsed["array"][0]
      tx.get(oid, "array.1").should eq parsed["array"][1]
      tx.get!(oid, "array.1.0").should eq parsed["array"][1][0]
      tx.get!(oid, "array.1.1").should eq parsed["array"][1][1]
      tx.get(oid, "array.2").should eq parsed["array"][2]
      tx.get!(oid, "array.2.0").should eq parsed["array"][2][0]

      tx.has_key?(oid, "null").should eq true
      tx.has_key!(oid, "null").should eq true
      tx.has_key?(oid, "dict").should eq true
      tx.has_key!(oid, "dict").should eq false
      tx.has_key?(oid, "nonexistent.key").should eq false
      tx.has_key!(oid, "nonexistent.key").should eq false

      tx.oids.should eq [oid]

      # indexes work for simple values as well as for arrays

      tx.where("dict.boolean", false).should eq [oid]
      tx.where("dict.boolean", true).should eq [] of Trove::Oid
      tx.where("dict.hello.0", "number").should eq [oid]
      tx.where("dict.hello", "number").should eq [oid]

      # where! is faster than where,
      # but returns only one value

      tx.where!("dict.boolean", false).should eq oid
      tx.where!("dict.hello", "number").should eq oid
      tx.where!("dict.hello", 42_i64).should eq oid

      tx.delete! oid, "dict.hello"
      tx.get(oid, "dict.hello").should eq ["number", 42, -4.2, 0.0]

      tx.delete! oid, "dict.hello.2"
      tx.get(oid, "dict.hello.2").should eq nil
      tx.get(oid, "dict.hello").should eq ["number", 42, 0.0]
      tx.where("dict.hello.2", -4.2).should eq [] of Trove::Oid
      tx.where("dict.hello", -4.2).should eq [] of Trove::Oid

      tx.delete oid, "dict.hello"
      tx.get(oid, "dict.hello").should eq nil
      tx.get(oid, "dict").should eq({"boolean" => false})

      tx.delete! oid, "dict.boolean"
      tx.where("dict.boolean", false).should eq [] of Trove::Oid
      tx.get(oid, "dict").should eq nil
      tx.get(oid).should eq({"null" => nil, "array" => [1, ["two", false], [nil]]})

      tx.set oid, "dict", parsed["dict"]
      tx.get(oid, "dict").should eq parsed["dict"]
      tx.set oid, "dict.boolean", JSON.parse %({"a": "b", "c": 4})
      tx.get(oid, "dict.boolean").should eq({"a" => "b", "c" => 4})

      # set! works when overwriting simple values

      tx.set! oid, "dict.null", parsed["array"]
      tx.get(oid, "dict.null").should eq parsed["array"]
    end

    s = Trove::A
    begin
      chest.transaction do |tx|
        s = tx.get(oid, "dict").not_nil!
        tx.delete oid, "dict"
        raise "oh no"
        tx << s
      end
    rescue ex
      ex.message.should eq "oh no"
      chest.transaction { |tx| tx.get(oid, "dict").should eq s }
    end

    chest.transaction do |tx|
      tx.delete oid
      tx.get(oid).should eq nil
      tx.get(oid, "null").should eq nil
      tx.oids.should eq [] of Trove::Oid

      tx.set oid, "", parsed
      tx.oids.should eq [oid]

      tx.delete oid
      tx.get(oid).should eq nil
      tx.get(oid, "null").should eq nil
      tx.where("dict.boolean", false).should eq [] of Trove::Oid
      tx.where("dict", false).should eq [] of Trove::Oid
      tx.oids.should eq [] of Trove::Oid
    end
  end

  it "supports dots in keys" do
    p = JSON.parse %({"a.b.c": 1})
    chest.transaction do |tx|
      i = tx << p
      tx.get(i).should eq p
      tx.delete i
    end
  end

  it "supports removing first array element" do
    p = JSON.parse %(["a", "b", "c"])
    chest.transaction do |tx|
      i = tx << p
      tx.delete! i, "0"
      tx.get(i).should eq ["b", "c"]
      tx.set! i, "k", JSON.parse %("a")
      tx.get(i).should eq({"k" => "a", "1" => "b", "2" => "c"})
      tx.delete i
    end
  end

  it "supports indexing large values" do
    l = 1024
    chest.transaction do |tx|
      (l - 32).upto (l + 32) do |size|
        v = ["a" * size]
        j = v.to_json
        p = JSON.parse j
        i = tx << p
        oids = [] of Trove::Oid
        tx.where("0", v.first) { |oid| oids << oid }
        oids.should eq [i]
        tx.get(i).should eq v
        tx.delete i
      end
    end
  end

  it "distinguishes in key/value pairs with same concatenaction result" do
    chest.transaction do |tx|
      i0 = tx << JSON.parse %({"as": "a"})
      i1 = tx << JSON.parse %({"a": "sa"})
      tx.where("as", "a").should eq [i0]
      tx.where("a", "sa").should eq [i1]
      tx.delete i0
      tx.delete i1
    end
  end

  it "can dump and load data" do
    # dump is gzip compressed json lines of format
    # {"oid": <object identifier>, "data": <object>}

    o0 = {"a" => "b"}
    o1 = COMPLEX_STRUCTURE
    chest.transaction do |tx|
      i0 = tx << JSON.parse o0.to_json
      i1 = tx << JSON.parse o1.to_json

      dump = IO::Memory.new
      tx.dump dump

      tx.delete i0
      tx.delete i1
      tx.get(i0).should eq nil
      tx.get(i1).should eq nil

      dump.rewind
      tx.load dump

      tx.get(i0).should eq o0
      tx.get(i1).should eq o1
      tx.delete i0
      tx.delete i1
    end
  end

  [
    "string",
    1234_i64,
    1234.1234_f64,
    -1234_i64,
    -1234.1234_f64,
    0_i64,
    0.0_f64,
    true,
    false,
    nil,
    {"key" => "value"},
    {"a" => "b", "c" => "d"},
    {"a" => {"b" => "c"}},
    {"a" => {"b" => {"c" => "d"}}},
    {"a" => {"b" => {"c" => "d"}}},
    ["a", "b", "c"],
    ["a"],
    [1_i64, 2_i64, 3_i64],
    [1_i64],
    ["a", 1_i64, true, 0.0_f64],
    COMPLEX_STRUCTURE,
  ].each do |o|
    it "add+get+where+delete #{o}" do
      j = o.to_json
      p = JSON.parse j
      chest.transaction do |tx|
        i = tx << p
        tx.get(i).should eq o

        tx.has_key?(i).should eq true

        case o
        when String, Int64, Float64, Bool, Nil
          tx.where("", o).should eq [i]
          tx.has_key!(i).should eq true
        when Array
          o.each_with_index do |v, k|
            tx.has_key!(i, k.to_s).should eq true
            tx.where(k.to_s, v).should eq [i]
          end
        when Hash(String, String)
          o.each do |k, v|
            tx.has_key!(i, k).should eq true
            tx.where(k.to_s, v).should eq [i]
          end
        when COMPLEX_STRUCTURE
          tx.has_key!(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq true
          tx.get(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq "This is six levels deep"
          tx.get!(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq "This is six levels deep"
          tx.where("level1.level2.level3.1.metadata.level4.level5.level6.note", "This is six levels deep").should eq [i]
        end

        tx.delete i
        tx.has_key!(i).should eq false
        tx.get(i).should eq nil
      end
    end
  end
end

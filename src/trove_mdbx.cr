require "json"
require "yaml"
require "uuid"
require "compress/gzip"

require "xxhash128"
require "mdbx"

module Trove
  alias Oid = Bytes
  alias A = JSON::Any
  alias H = Hash(String, A)
  alias AA = Array(A)

  class Chest
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter env : Mdbx::Env

    def initialize(@env)
    end

    def transaction(&)
      @env.transaction { |tx| yield Transaction.new tx }
    end
  end

  class Transaction
    getter tx : Mdbx::Transaction
    getter d : Mdbx::Db
    getter i : Mdbx::Db
    getter u : Mdbx::Db
    getter o : Mdbx::Db

    def initialize(@tx)
      @d = @tx.db @tx.dbi "d"
      @i = @tx.db @tx.dbi "i"
      @u = @tx.db @tx.dbi "u"
      @o = @tx.db @tx.dbi "o"
    end

    protected def new_oid : Oid
      UUID.v7.bytes.to_slice.clone
    end

    protected def digest(data : Bytes)
      d = LibXxhash.xxhash128 data, data.size, 0
      r = Slice(UInt64).new 2
      r[0] = d.high64
      r[1] = d.low64
      r.to_unsafe.as(UInt8*).to_slice 16
    end

    protected def digest(pb : String, ve : String)
      digest [pb, ve].to_json.to_slice
    end

    def oids(&)
      @tx.db(@o).each { |o, _| yield o }
    end

    def oids
      @tx.db(@o).all.map { |o, _| o }
    end

    macro mwo
      o = if flat.size == 0
            nil
          elsif flat.has_key? ""
            flat[""]
          else
            h2a A.new nest flat
          end

      gzip.puts({"oid"  => oid.hexstring,
                 "data" => o}.to_json)
    end

    def dump(io : IO)
      Compress::Gzip::Writer.open(io, Compress::Deflate::BEST_COMPRESSION) do |gzip|
        oid : Oid? = nil
        flat = H.new
        @d.each do |k, v|
          i = k[..15]
          unless i == oid
            if oid
              mwo
              flat.clear
            end
            oid = i
          end
          flat[d[:dp]] = A.new decode v
        end
        if oid
          mwo
        end
      end
    end

    def load(io : IO)
      Compress::Gzip::Reader.open(io) do |gzip|
        gzip.each_line do |l|
          p = JSON.parse l.chomp
          set(p["oid"].as_s.hexbytes, "", p["data"])
        end
      end
    end

    alias I = String | Int64 | Float64 | Bool | Nil

    protected def encode(v : I) : String
      case v
      when String
        "s#{v}"
      when Int64
        "i#{v}"
      when Float64
        "f#{v}"
      when true
        "T"
      when false
        "F"
      when nil
        ""
      else
        raise "Can not encode #{v}"
      end
    end

    protected def decode(s : String) : I
      return nil if s.empty?
      case s[0]
      when 's'
        s[1..]
      when 'i'
        s[1..].to_i64
      when 'f'
        s[1..].to_f
      when 'T'
        true
      when 'F'
        false
      end
    end

    protected def partition(p : String)
      pp = p.rpartition '.'
      {b: pp[0], i: pp[2].to_u32} rescue {b: p, i: 0_u32}
    end

    protected def set(i : Oid, p : String, o : A::Type)
      case o
      when H
        o.each do |k, v|
          ke = k.gsub(".", "\\.")
          set i, p.empty? ? ke : "#{p}.#{ke}", v.raw
        end
        return
      when AA
        o.each_with_index { |v, k| set i, p.empty? ? k.to_s : "#{p}.#{k}", v.raw }
        return
      else
        oe = encode o
      end
      @d.upsert i + p.to_slice, oe.to_slice
      pp = partition p
      ppib = begin
        r = Slice(UInt32).new 1
        r[0] = pp[:i]
        r.to_unsafe.as(UInt8*).to_slice 4
      end
      d = digest pp[:b], oe
      @i.upsert d + ppib + i, Bytes.new 0
      @u.upsert d, i
    end

    def set(i : Oid, p : String, o : A)
      delete i, p unless p.empty? && !@o.get i
      @o.upsert i, Bytes.new 0
      set i, p, o.raw
    end

    protected def deletei(i : Oid, p : String)
      pp = partition p
      d = digest pp[:b], (@d.get(i + p.to_slice).not_nil! rescue return)
      @i.delete d + begin
        r = Slice(UInt32).new 1
        r[0] = pp[:i]
        r.to_unsafe.as(UInt8*).to_slice 4
      end + i
      @u.delete d
    end

    def set!(i : Oid, p : String, o : A)
      deletei i, p
      @o.upsert i
      set i, p, o.raw
    end

    def <<(o : A)
      i = new_oid
      set i, "", o
      i
    end

    protected def h2a(a : A) : A
      if ah = a.as_h?
        if ah.keys.all? { |k| k.to_u64 rescue nil }
          vs = ah.values
          return A.new AA.new(ah.size) { |i| h2a vs[i] }
        else
          ah.each { |k, v| ah[k] = h2a v }
        end
      end
      a
    end

    protected def nest(h : H)
      r = H.new
      h.each do |p, v|
        ps = p.split /(?<!\\)\./
        c = r

        ps.each_with_index do |ke, i|
          k = ke.gsub("\\.", ".")
          if i == ps.size - 1
            c[k] = v
          else
            c[k] ||= A.new H.new
            c = c[k].as_h
          end
        end
      end
      r
    end

    def has_key?(i : Oid, p : String = "")
      @d.from i do |k, _|
        st = i + p.to_slice
        return k.size >= st.size && k[..st.size - 1] == st
      end
      false
    end

    def has_key!(i : Oid, p : String = "")
      @d.get(i + p.to_slice) != nil
    end

    def get(i : Oid, p : String = "")
      flat = H.new
      st = i + p.to_slice
      @d.from(st) do |k, o|
        break unless k.size >= st.size && k[..st.size - 1] == st
        flat[String.new(k[16..]).lchop(p).lchop('.')] = A.new decode String.new o
      end
      return nil if flat.size == 0
      return flat[""] if flat.has_key? ""
      h2a A.new nest flat
    end

    def get!(i : Oid, p : String)
      decode String.new(@d.get(i + p.to_slice).not_nil!) rescue nil
    end

    protected def delete(i : Oid, p : String, ve : String)
      @d.delete i + p.to_slice
      pp = partition p
      d = digest pp[:b], ve
      @i.delete d + begin
        r = Slice(UInt32).new 1
        r[0] = pp[:i]
        r.to_unsafe.as(UInt8*).to_slice 4
      end + i
      @u.delete d
    end

    def delete(i : Oid, p : String = "")
      @o.delete i if p.empty?
      st = i + p.to_slice
      @d.from(i + p.to_slice) do |k, o|
        break unless k.size >= st.size && k[..st.size - 1] == st
        delete i, String.new(k[16..]), String.new(o)
      end
    end

    def delete!(i : Oid, p : String = "")
      delete i, p, (String.new @d.get(i + p.to_slice).not_nil! rescue return)
    end

    def where(p : String, v : I, &)
      pp = partition p
      d = digest pp[:b], encode v
      @i.from(d, begin
        r = Slice(UInt32).new 1
        r[0] = pp[:i]
        r.to_unsafe.as(UInt8*).to_slice 4
      end) do |k, v|
        break unless k[..15] == d
        yield k[..15]
      end
    end

    def where(p : String, v : I)
      r = [] of Trove::Oid
      where(p, v) { |i| r << i }
      r
    end

    def unique(p : String, v : I)
      @u.get digest p, encode v
    end
  end
end

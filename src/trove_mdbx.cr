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

  struct Transaction
    getter tx : Mdbx::Transaction
    getter d : Mdbx::Db
    getter i : Mdbx::Db
    getter u : Mdbx::Db
    getter o : Mdbx::Db

    def initialize(@tx)
      @d = @tx.db 2
      @i = @tx.db 3
      @u = @tx.db 4
      @o = @tx.db 5
    end

    def transaction(&)
      @tx.transaction { |tx| yield Transaction.new tx }
    end

    protected def new_oid : Oid
      UUID.v7.bytes.to_slice.clone
    end

    protected def digest(data : Bytes)
      d = LibXxhash.xxhash128 data, data.size, 0
      pointerof(d).as(UInt8*).to_slice(16).clone
    end

    protected def digest(pb : Bytes, ve : Bytes)
      ds = Bytes.new pb.size + 1 + ve.size
      pb.copy_to ds.to_unsafe, pb.size
      ve.copy_to ds.to_unsafe + pb.size + 1, ve.size
      digest ds
    end

    protected def partition(p : Bytes)
      dl = p.rindex '.'.ord.to_u8
      {b: p[..dl.not_nil! - 1], i: String.new(p[dl.not_nil! + 1..]).to_u32} rescue {b: p, i: 0_u32}
    end

    protected def ike(d : Bytes, i : UInt32, oid : Bytes)
      r = Bytes.new 36
      d.copy_to r.to_unsafe, 16
      IO::ByteFormat::LittleEndian.encode i, r[16..]
      oid.copy_to r.to_unsafe + 20, 16
      r
    end

    def oids(&)
      @tx.db(@o).each { |o, _| yield o }
    end

    def oids
      @o.all.map { |o, _| o }
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
          flat[String.new k[16..]] = A.new decode v
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

    protected def encode(v : I) : Bytes
      case v
      when String
        r = Bytes.new 1 + v.bytesize
        r[0] = {{'s'.ord}}.to_u8
        v.to_unsafe.copy_to r.to_unsafe + 1, v.bytesize
        r
      when Int64
        r = Bytes.new 1 + 8
        r[0] = {{'i'.ord}}.to_u8
        IO::ByteFormat::LittleEndian.encode(v, r[1..])
        r
      when Float64
        r = Bytes.new 1 + 8
        r[0] = {{'f'.ord}}.to_u8
        IO::ByteFormat::LittleEndian.encode(v, r[1..])
        r
      when true  then Bytes.new 1, {{'T'.ord}}.to_u8
      when false then Bytes.new 1, {{'F'.ord}}.to_u8
      when nil   then Bytes.empty
      else            raise "Can not encode #{v}"
      end
    end

    protected def decode(b : Bytes) : I
      return nil if b.empty?
      case b[0]
      when {{'s'.ord}} then String.new b[1..]
      when {{'i'.ord}} then IO::ByteFormat::LittleEndian.decode(Int64, b[1..])
      when {{'f'.ord}} then IO::ByteFormat::LittleEndian.decode(Float64, b[1..])
      when {{'T'.ord}} then true
      when {{'F'.ord}} then false
      end
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
      pp = partition p.to_slice
      d = digest pp[:b], oe

      ik = ike d, pp[:i], i
      @i.upsert ik, Bytes.empty
      @u.upsert d, i
    end

    def set(i : Oid, p : String, o : A)
      delete i, p unless p.empty? && !@o.get i
      @o.upsert i, Bytes.empty
      set i, p, o.raw
    end

    protected def deletei(i : Oid, p : String)
      pp = partition p.to_slice
      d = digest pp[:b], (@d.get(i + p.to_slice).not_nil! rescue return)

      @i.delete ike d, pp[:i], i
      @u.delete d
    end

    def set!(i : Oid, p : String, o : A)
      deletei i, p
      @o.upsert i, Bytes.empty
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
      st = i + p.to_slice
      @d.from st do |k, _|
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
      @d.from st do |k, o|
        break unless k.size >= st.size && k[..st.size - 1] == st
        flat[String.new(k[16..]).lchop(p).lchop('.')] = A.new decode o
      end
      return nil if flat.size == 0
      return flat[""] if flat.has_key? ""
      h2a A.new nest flat
    end

    def get!(i : Oid, p : String)
      decode @d.get(i + p.to_slice).not_nil! rescue nil
    end

    protected def delete(i : Oid, p : Bytes, ve : Bytes)
      st = i + p.to_slice
      pp = partition p
      d = digest pp[:b], ve

      raise "Index record not found for #{i} #{p} #{ve}" unless @i.delete ike d, pp[:i], i
      @d.delete st
      @u.delete d
    end

    def delete(i : Oid, p : String = "")
      @o.delete i if p.empty?
      st = i + p.to_slice
      @d.from st do |k, o|
        break unless k.size >= st.size && k[..st.size - 1] == st
        delete i, k[16..], o
      end
    end

    def delete!(i : Oid, p : String = "")
      delete i, p.to_slice, @d.get(i + p.to_slice).not_nil! rescue return
    end

    def where(p : String, v : I, &)
      pp = partition p.to_slice
      d = digest pp[:b], encode v

      ik = Bytes.new 20
      d.copy_to ik.to_unsafe, 16
      IO::ByteFormat::LittleEndian.encode pp[:i], ik[16..]

      @i.from ik do |k, _|
        break unless k[..15] == d
        yield k[-16..]
      end
    end

    def where(p : String, v : I)
      r = [] of Trove::Oid
      where(p, v) { |i| r << i }
      r
    end

    def unique(p : String, v : I)
      @u.get digest p.to_slice, encode v
    end
  end
end

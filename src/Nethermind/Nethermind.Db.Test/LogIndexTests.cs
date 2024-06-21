using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using FluentAssertions;
using Nethermind.Core;
using NUnit.Framework;

namespace Nethermind.Db.Test
{

    public class LogIndexTests
    {

        [Test]
        public void Can_get_all_on_empty()
        {
            IDb logIndexDb = new MemDb();
            Address x = Address.Zero;

            Span<byte> key = stackalloc byte[24];
            x.Bytes.CopyTo(key);
            Span<byte> last4bytes = key[20..];
            BinaryPrimitives.WriteInt32BigEndian(last4bytes, 10_000_00);
            Span<byte> value = [1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13, 14, 15, 16, 17, 26];
            byte[] delta = [1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 9];
            ILogEncoder<byte, byte> encoder = new FastPForEncoder(8);
            ILogDecoder<byte, byte> decoder = new FastPForDecoder(8);

            byte[] encoded = new byte[value.Length];
            encoder.Encode(value, encoded);

            logIndexDb.PutSpan(key, encoded);

            var result = logIndexDb.GetAll();

            var raw_bytes = logIndexDb.Get(key);



            var decodedValue = new byte[delta.Length];
            decoder.Decode(raw_bytes, decodedValue);
            decodedValue.Should().BeEquivalentTo(value.ToArray());

            foreach (KeyValuePair<byte[], byte[]> item in result)
            {
                item.Value.Should().BeEquivalentTo(delta);
            }

            result.Should().NotBeEmpty();
        }
    }
}

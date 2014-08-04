using System;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;

namespace KafkaNet.Common
{
    /// <summary>
    /// Provides Big Endian conversion extensions to required types for the Kafka protocol.
    /// </summary>
    public static class Extensions
    {
		private static readonly Encoding StringEncoding = new UTF8Encoding(false);

        public static byte[] ToIntSizedBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();

            return value.Length.ToBytes()
                        .Concat(value.ToUnsizedBytes())
                        .ToArray();
        }

		/// <summary>
		/// Convert to a Kafka protocol string (ASCII encoded, prefixed with Int16 size of string)
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static byte[] ToInt16SizedBytes(this string value)
        {
			if (string.IsNullOrEmpty(value))
			{
				return ((short)-1).ToBytes();
			}

			var bytes = value.ToUnsizedBytes();

            return ((Int16)bytes.Length).ToBytes()
                        .Concat(bytes)
                        .ToArray();
        }

		/// <summary>
		/// Convert to bytes with the UTF8 encoding. 
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
        public static byte[] ToUnsizedBytes(this string value)
        {
			if (value == null)
			{
				return null;
			}
			else if (value.Length == 0)
			{
				return new byte[0];
			}
			else
			{
				return StringEncoding.GetBytes(value);
			}
        }

        public static byte[] ToBytes(this Int16 value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this Int32 value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this Int64 value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this float value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this double value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this char value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this bool value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }
        
        public static Int32 ToInt32(this byte[] value)
        {
            return BitConverter.ToInt32(value.Reverse().ToArray(), 0);
        }

		public static byte[] ToIntPrefixedBytes(this byte[] value)
		{
			if (value == null || value.Length == 0)
			{
				return (-1).ToBytes();
			}

			return value.Length.ToBytes()
						.Concat(value)
						.ToArray();
		}
    }
}

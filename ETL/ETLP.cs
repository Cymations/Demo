using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ETL
{
	// Interface for extraction
	public interface IExtractor<T>
	{
		IEnumerable<T> Extract(string filePath);
		// Optionally: IAsyncEnumerable<T> ExtractAsync(string filePath);
	}

	// Interface for transformation
	public interface ITransformer<TIn, TOut>
	{
		IEnumerable<TOut> Transform(IEnumerable<TIn> input);
	}

	// Interface for loading
	public interface ILoader<T>
	{
		void Load(IEnumerable<T> data, string filePath);
	}

	// Extractor for CSV files (skeleton)
	public class Extract : IExtractor<Dictionary<string, string>>
	{
		public IEnumerable<Dictionary<string, string>> LoadCsv(string filePath)
		{
			// TODO: Implement CSV extraction logic
			throw new NotImplementedException();
		}

		IEnumerable<Dictionary<string, string>> IExtractor<Dictionary<string, string>>.Extract(string filePath)
		{
			return LoadCsv(filePath);
		}
	}

	// Transformer (skeleton)
	public class Transform : ITransformer<Dictionary<string, string>, Dictionary<string, string>>
	{

		public IEnumerable<Dictionary<string, string>> TransformData(IEnumerable<Dictionary<string, string>> input)
		{
			// TODO: Implement transformation logic
			throw new NotImplementedException();
		}


		/// <summary>
		/// Attempts to parse and normalize a BatchId token. Does not throw; returns true if valid, false otherwise.
		/// </summary>
		/// <param name="batchId">The input BatchId string (expected format: yyyyMMdd-SSSS).</param>
		/// <param name="normalized">The normalized BatchId if valid, or null if invalid.</param>
		/// <returns>True if parsing and normalization succeeded, false otherwise.</returns>
		public static bool TryParseBatchId(string batchId, out string? normalized)
		{
			normalized = null;
			if (string.IsNullOrEmpty(batchId) || batchId.Length != 13 || batchId[8] != '-')
				return false;

			// Fast path: check digits for date and sequence
			for (int i = 0; i < 8; i++)
				if (batchId[i] < '0' || batchId[i] > '9')
					return false;
			for (int i = 9; i < 13; i++)
				if (batchId[i] < '0' || batchId[i] > '9')
					return false;

			// Validate date
			int year = (batchId[0] - '0') * 1000 + (batchId[1] - '0') * 100 + (batchId[2] - '0') * 10 + (batchId[3] - '0');
			int month = (batchId[4] - '0') * 10 + (batchId[5] - '0');
			int day = (batchId[6] - '0') * 10 + (batchId[7] - '0');
			if (month < 1 || month > 12 || day < 1 || day > 31)
				return false;
			// Use DateTime.TryParseExact for full validation (no allocation)
			if (!System.DateTime.TryParseExact(batchId.Substring(0, 8), "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out _))
				return false;

			// Validate sequence
			int seq = (batchId[9] - '0') * 1000 + (batchId[10] - '0') * 100 + (batchId[11] - '0') * 10 + (batchId[12] - '0');
			if (seq < 1 || seq > 9999)
				return false;

			normalized = NormalizeBatchIdToken(batchId);
			return true;
		}

		/// <summary>
		/// Validates a BatchId token and returns rich error codes/messages.
		/// </summary>
		/// <param name="batchId">The input BatchId string (expected format: yyyyMMdd-SSSS).</param>
		/// <returns>A tuple: IsValid, List of error codes/messages, and DebugInfo string.</returns>
		public static (bool IsValid, List<(string Code, string Message)> Errors, string DebugInfo) ValidateBatchId(string batchId)
		{
			var errors = new List<(string, string)>();
			if (string.IsNullOrEmpty(batchId))
			{
				errors.Add(("ERR_NULL_OR_EMPTY", "BatchId is null or empty."));
				return (false, errors, "Input is null or empty");
			}
			if (batchId.Length != 13 || batchId[8] != '-')
				errors.Add(("ERR_FORMAT", "BatchId must be 13 characters and contain a hyphen at position 9 (yyyyMMdd-SSSS)."));
			for (int i = 0; i < 8 && batchId.Length > i; i++)
				if (batchId[i] < '0' || batchId[i] > '9')
					errors.Add(("ERR_DATE_DIGIT", $"Character {i + 1} of BatchId must be a digit."));
			for (int i = 9; i < 13 && batchId.Length > i; i++)
				if (batchId[i] < '0' || batchId[i] > '9')
					errors.Add(("ERR_SEQ_DIGIT", $"Character {i + 1} of BatchId must be a digit."));
			if (batchId.Length >= 8)
			{
				int year = 0, month = 0, day = 0;
				if (batchId.Length >= 4)
					year = (batchId[0] - '0') * 1000 + (batchId[1] - '0') * 100 + (batchId[2] - '0') * 10 + (batchId[3] - '0');
				if (batchId.Length >= 6)
					month = (batchId[4] - '0') * 10 + (batchId[5] - '0');
				if (batchId.Length >= 8)
					day = (batchId[6] - '0') * 10 + (batchId[7] - '0');
				if (month < 1 || month > 12)
					errors.Add(("ERR_MONTH", "Month must be between 01 and 12."));
				if (day < 1 || day > 31)
					errors.Add(("ERR_DAY", "Day must be between 01 and 31."));
				if (batchId.Length >= 8 && !System.DateTime.TryParseExact(batchId.Substring(0, 8), "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out _))
					errors.Add(("ERR_DATE", "Date portion is not a valid calendar date."));
			}
			if (batchId.Length >= 13)
			{
				int seq = (batchId[9] - '0') * 1000 + (batchId[10] - '0') * 100 + (batchId[11] - '0') * 10 + (batchId[12] - '0');
				if (seq < 1 || seq > 9999)
					errors.Add(("ERR_SEQ_RANGE", "Sequence must be between 0001 and 9999."));
			}
			var debug = $"Checked BatchId: {batchId}, Errors: {string.Join(", ", errors)}";
			return (errors.Count == 0, errors, debug);
		}

		/// <summary>
		/// Normalizes a BatchId token to the canonical format (yyyyMMdd-SSSS, upper-case, zero-padded sequence).
		/// </summary>
		/// <param name="batchId">The input BatchId string.</param>
		/// <returns>The normalized BatchId string, or null if input is invalid.</returns>
		/// <summary>
		/// Normalizes a BatchId token to the canonical format (yyyyMMdd-SSSS, upper-case, zero-padded sequence).
		/// Assumes input is valid. Does not perform validation.
		/// </summary>
		/// <param name="batchId">The input BatchId string (must be valid).</param>
		/// <returns>The normalized BatchId string, or null if input is null or not 13 chars.</returns>
		public static string? NormalizeBatchIdToken(string batchId)
		{
			var (isValid, _, _) = ValidateBatchId(batchId);
			if (!isValid)
				return null;
			return batchId.Substring(0, 8) + "-" + batchId.Substring(9, 4);
		}

		IEnumerable<Dictionary<string, string>> ITransformer<Dictionary<string, string>, Dictionary<string, string>>.Transform(IEnumerable<Dictionary<string, string>> input)
		{
			return TransformData(input);
		}
	}

	// Loader for CSV files (skeleton)
	public class Load : ILoader<Dictionary<string, string>>
	{
		public void SaveCsv(IEnumerable<Dictionary<string, string>> data, string filePath)
		{
			// TODO: Implement CSV loading logic
			throw new NotImplementedException();
		}

		void ILoader<Dictionary<string, string>>.Load(IEnumerable<Dictionary<string, string>> data, string filePath)
		{
			SaveCsv(data, filePath);
		}
	}

	// Pipeline orchestrator (skeleton)
	public class Pipeline
	{
		private readonly IExtractor<Dictionary<string, string>> _extractor;
		private readonly ITransformer<Dictionary<string, string>, Dictionary<string, string>> _transformer;
		private readonly ILoader<Dictionary<string, string>> _loader;

		public Pipeline(
			IExtractor<Dictionary<string, string>> extractor,
			ITransformer<Dictionary<string, string>, Dictionary<string, string>> transformer,
			ILoader<Dictionary<string, string>> loader)
		{
			_extractor = extractor;
			_transformer = transformer;
			_loader = loader;
		}

		public void Run(string inputFilePath, string outputFilePath)
		{
			var extracted = _extractor.Extract(inputFilePath);
			var transformed = _transformer.Transform(extracted);
			_loader.Load(transformed, outputFilePath);
		}
	}
}

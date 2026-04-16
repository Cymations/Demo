using System;
using System.Collections.Generic;
using Xunit;
using ETL;

public class BatchIdTests
{
	internal static class BatchIdErrorCodes
	{
		public const string NullOrEmpty = "ERR_NULL_OR_EMPTY";
		public const string SeqRange = "ERR_SEQ_RANGE";
		public const string Format = "ERR_FORMAT";
		public const string SeqDigit = "ERR_SEQ_DIGIT";
		public const string Month = "ERR_MONTH";
		public const string Date = "ERR_DATE";
		public const string DateDigit = "ERR_DATE_DIGIT";
	}

	public static IEnumerable<object[]> ValidBatchIds => new List<object[]>
	{
		new object[] { "20260406-0001", "20260406-0001" },
		new object[] { "20231231-9999", "20231231-9999" },
		new object[] { "20200101-0100", "20200101-0100" },
	};

	public static IEnumerable<object[]> InvalidBatchIds => new List<object[]>
	{
		new object[] { null!, new[] { BatchIdErrorCodes.NullOrEmpty } },
		new object[] { "", new[] { BatchIdErrorCodes.NullOrEmpty } },
		new object[] { "20260406-0000", new[] { BatchIdErrorCodes.SeqRange } },
		new object[] { "20260406-10000", new[] { BatchIdErrorCodes.Format } },
		new object[] { "2026046-0001", new[] { BatchIdErrorCodes.Format } },
		new object[] { "20260406-0A01", new[] { BatchIdErrorCodes.SeqDigit } },
		new object[] { "20261301-0001", new[] { BatchIdErrorCodes.Month, BatchIdErrorCodes.Date } },
		new object[] { "20260230-0001", new[] { BatchIdErrorCodes.Date } },
		new object[] { "20260406-", new[] { BatchIdErrorCodes.Format } },
	};

	public static IEnumerable<object[]> NullBatchIds => new List<object[]>
	{
		new object[] { null!, new[] { BatchIdErrorCodes.NullOrEmpty } },
	};

	public static IEnumerable<object[]> EmptyBatchIds => new List<object[]>
	{
		new object[] { "", new[] { BatchIdErrorCodes.NullOrEmpty } },
	};

	public static IEnumerable<object[]> BoundaryBatchIds => new List<object[]>
	{
		new object[] { "20260101-0001", "20260101-0001" }, // Earliest valid
		new object[] { "20261231-9999", "20261231-9999" }, // Latest valid
	};

	   public static IEnumerable<object[]> WhitespaceBatchIds => new List<object[]>
	   {
		   new object[] { "   ", new[] { BatchIdErrorCodes.Format } },
		   new object[] { "20260406-   ", new[] { BatchIdErrorCodes.SeqDigit } },
		   new object[] { "        -0001", new[] {
			   BatchIdErrorCodes.DateDigit, BatchIdErrorCodes.DateDigit, BatchIdErrorCodes.DateDigit, BatchIdErrorCodes.DateDigit,
			   BatchIdErrorCodes.DateDigit, BatchIdErrorCodes.DateDigit, BatchIdErrorCodes.DateDigit, BatchIdErrorCodes.DateDigit
		   } },
	   };

	   public static IEnumerable<object[]> WhitespaceBatchIdsForNormalization => new List<object[]>
	   {
		   new object[] { "   ", null },
		   new object[] { "20260406-   ", null },
		   new object[] { "        -0001", "        -0001" },
	   };

	[Theory]
	[MemberData(nameof(ValidBatchIds))]
	public void TryParseBatchId_WithValidInput_ReturnsTrueAndNormalized(string input, string expectedNormalized)
	{
		var result = Transform.TryParseBatchId(input, out var normalized);
		Assert.True(result);
		Assert.Equal(expectedNormalized, normalized);
	}

	[Theory]
	[MemberData(nameof(InvalidBatchIds))]
	public void TryParseBatchId_WithInvalidInput_ReturnsFalseOrThrows(string input, string[] _)
	{
		if (input == null)
		{
			Assert.Throws<ArgumentNullException>(() => Transform.TryParseBatchId(input!, out var _));
		}
		else
		{
			var result = Transform.TryParseBatchId(input, out var normalized);
			Assert.False(result);
			Assert.Null(normalized);
		}
	}

	[Theory]
	[MemberData(nameof(NullBatchIds))]
	public void TryParseBatchId_WithNullInput_ThrowsArgumentNullException(string input, string[] _)
	{
		Assert.Throws<ArgumentNullException>(() => Transform.TryParseBatchId(input!, out var _));
	}

	[Theory]
	[MemberData(nameof(EmptyBatchIds))]
	public void TryParseBatchId_WithEmptyInput_ReturnsFalse(string input, string[] _)
	{
		var result = Transform.TryParseBatchId(input, out var normalized);
		Assert.False(result);
		Assert.Null(normalized);
	}

	[Theory]
	[MemberData(nameof(ValidBatchIds))]
	public void NormalizeBatchIdToken_WithValidInput_ReturnsNormalized(string input, string expectedNormalized)
	{
		var normalized = Transform.NormalizeBatchIdToken(input);
		Assert.Equal(expectedNormalized, normalized);
	}

	[Theory]
	[MemberData(nameof(InvalidBatchIds))]
	public void NormalizeBatchIdToken_WithInvalidInput_ReturnsNullOrThrows(string input, string[] _)
	{
		if (input == null)
		{
			Assert.Throws<ArgumentNullException>(() => Transform.NormalizeBatchIdToken(input!));
		}
		else if (input.Length != 13)
		{
			var normalized = Transform.NormalizeBatchIdToken(input);
			Assert.Null(normalized);
		}
		else
		{
			var normalized = Transform.NormalizeBatchIdToken(input);
			Assert.NotNull(normalized);
		}
	}

	[Theory]
	[MemberData(nameof(NullBatchIds))]
	public void NormalizeBatchIdToken_WithNullInput_ThrowsArgumentNullException(string input, string[] _)
	{
		Assert.Throws<ArgumentNullException>(() => Transform.NormalizeBatchIdToken(input!));
	}

	[Theory]
	[MemberData(nameof(EmptyBatchIds))]
	public void NormalizeBatchIdToken_WithEmptyInput_ReturnsNull(string input, string[] _)
	{
		var normalized = Transform.NormalizeBatchIdToken(input);
		Assert.Null(normalized);
	}

	[Theory]
	[MemberData(nameof(ValidBatchIds))]
	public void ValidateBatchId_WithValidInput_ReturnsValid(string input, string _)
	{
		var (isValid, errors, debug) = Transform.ValidateBatchId(input);
		Assert.True(isValid);
		Assert.Empty(errors);
		Assert.Contains("Checked BatchId", debug);
	}

	[Theory]
	[MemberData(nameof(InvalidBatchIds))]
	public void ValidateBatchId_WithInvalidInput_ReturnsErrorsOrThrows(string input, string[] expectedErrorCodes)
	{
		if (input == null)
		{
			Assert.Throws<ArgumentNullException>(() => Transform.ValidateBatchId(input!));
		}
		else
		{
			var (isValid, errors, debug) = Transform.ValidateBatchId(input);
			Assert.False(isValid);
			foreach (var code in expectedErrorCodes)
			{
				Assert.Contains(errors, e => e.Code == code);
			}
			if (input.Length > 0)
				Assert.Contains("Checked BatchId", debug);
			else
				Assert.Contains("Input is empty", debug);
		}
	}

	[Theory]
	[MemberData(nameof(NullBatchIds))]
	public void ValidateBatchId_WithNullInput_ThrowsArgumentNullException(string input, string[] _)
	{
		Assert.Throws<ArgumentNullException>(() => Transform.ValidateBatchId(input!));
	}

	[Theory]
	[MemberData(nameof(EmptyBatchIds))]
	public void ValidateBatchId_WithEmptyInput_ReturnsErrors(string input, string[] expectedErrorCodes)
	{
		var (isValid, errors, debug) = Transform.ValidateBatchId(input);
		Assert.False(isValid);
		foreach (var code in expectedErrorCodes)
		{
			Assert.Contains(errors, e => e.Code == code);
		}
		Assert.Contains("Input is empty", debug);
	}

	[Theory]
	[MemberData(nameof(BoundaryBatchIds))]
	public void TryParseBatchId_WithBoundaryValues_ReturnsTrueAndNormalized(string input, string expectedNormalized)
	{
		var result = Transform.TryParseBatchId(input, out var normalized);
		Assert.True(result);
		Assert.Equal(expectedNormalized, normalized);
	}

	[Theory]
	[MemberData(nameof(BoundaryBatchIds))]
	public void NormalizeBatchIdToken_WithBoundaryValues_ReturnsNormalized(string input, string expectedNormalized)
	{
		var normalized = Transform.NormalizeBatchIdToken(input);
		Assert.Equal(expectedNormalized, normalized);
	}

	[Theory]
	[MemberData(nameof(BoundaryBatchIds))]
	public void ValidateBatchId_WithBoundaryValues_ReturnsValid(string input, string _)
	{
		var (isValid, errors, debug) = Transform.ValidateBatchId(input);
		Assert.True(isValid);
		Assert.Empty(errors);
		Assert.Contains("Checked BatchId", debug);
	}

	[Theory]
	[MemberData(nameof(WhitespaceBatchIds))]
	public void TryParseBatchId_WithWhitespaceInput_ReturnsFalse(string input, string[] _)
	{
		var result = Transform.TryParseBatchId(input, out var normalized);
		Assert.False(result);
		Assert.Null(normalized);
	}


	[Theory]
	[MemberData(nameof(WhitespaceBatchIdsForNormalization))]
	public void NormalizeBatchIdToken_WithWhitespaceInput_ReturnsExpected(string input, string? expected)
	{
		var normalized = Transform.NormalizeBatchIdToken(input);
		Assert.Equal(expected, normalized);
	}

	[Theory]
	[MemberData(nameof(WhitespaceBatchIds))]
	public void ValidateBatchId_WithWhitespaceInput_ReturnsErrors(string input, string[] expectedErrorCodes)
	{
		var (isValid, errors, debug) = Transform.ValidateBatchId(input);
		Assert.False(isValid);
		foreach (var code in expectedErrorCodes)
		{
			Assert.Contains(errors, e => e.Code == code);
		}
	}

	[Fact]
	public void BatchId_Methods_ThrowArgumentNullException_OnNullInput()
	{
		Assert.Throws<ArgumentNullException>(() => Transform.TryParseBatchId(null!, out var _));
		Assert.Throws<ArgumentNullException>(() => Transform.ValidateBatchId(null!));
		Assert.Throws<ArgumentNullException>(() => Transform.NormalizeBatchIdToken(null!));
	}
}
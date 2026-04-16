using System;
using System.Collections.Generic;
using Xunit;
using ETL;

public class BatchIdTests
{
	public static IEnumerable<object[]> ValidBatchIds => new List<object[]>
	{
		new object[] { "20260406-0001", "20260406-0001" },
		new object[] { "20231231-9999", "20231231-9999" },
		new object[] { "20200101-0100", "20200101-0100" },
	};

	public static IEnumerable<object[]> InvalidBatchIds => new List<object[]>
	{
		new object[] { null, new[] { "ERR_NULL_OR_EMPTY" } },
		new object[] { "", new[] { "ERR_NULL_OR_EMPTY" } },
		new object[] { "20260406-0000", new[] { "ERR_SEQ_RANGE" } },
		new object[] { "20260406-10000", new[] { "ERR_FORMAT" } },
		new object[] { "2026046-0001", new[] { "ERR_FORMAT" } },
		new object[] { "20260406-0A01", new[] { "ERR_SEQ_DIGIT" } },
		new object[] { "20261301-0001", new[] { "ERR_MONTH", "ERR_DATE" } },
		new object[] { "20260230-0001", new[] { "ERR_DATE" } },
		new object[] { "20260406-", new[] { "ERR_FORMAT" } },
	};

	[Theory]
	[MemberData(nameof(ValidBatchIds))]
	public void TryParseBatchId_ValidInputs_ReturnsTrueAndNormalized(string input, string expectedNormalized)
	{
		var result = Transform.TryParseBatchId(input, out var normalized);
		Assert.True(result);
		Assert.Equal(expectedNormalized, normalized);
	}

	[Theory]
	[MemberData(nameof(InvalidBatchIds))]
	public void TryParseBatchId_InvalidInputs_ReturnsFalse(string input, string[] _)
	{
		var result = Transform.TryParseBatchId(input, out var normalized);
		Assert.False(result);
		Assert.Null(normalized);
	}

	[Theory]
	[MemberData(nameof(ValidBatchIds))]
	public void NormalizeBatchIdToken_ValidInputs_ReturnsNormalized(string input, string expectedNormalized)
	{
		var normalized = Transform.NormalizeBatchIdToken(input);
		Assert.Equal(expectedNormalized, normalized);
	}

	[Theory]
	[MemberData(nameof(InvalidBatchIds))]
	public void NormalizeBatchIdToken_InvalidInputs_ReturnsNull(string input, string[] _)
	{
		var normalized = Transform.NormalizeBatchIdToken(input);
		Assert.Null(normalized);
	}

	[Theory]
	[MemberData(nameof(ValidBatchIds))]
	public void ValidateBatchId_ValidInputs_ReturnsValid(string input, string _)
	{
		var (isValid, errors, debug) = Transform.ValidateBatchId(input);
		Assert.True(isValid);
		Assert.Empty(errors);
		Assert.Contains("Checked BatchId", debug);
	}

	[Theory]
	[MemberData(nameof(InvalidBatchIds))]
	public void ValidateBatchId_InvalidInputs_ReturnsErrors(string input, string[] expectedErrorCodes)
	{
		var (isValid, errors, debug) = Transform.ValidateBatchId(input);
		Assert.False(isValid);
		foreach (var code in expectedErrorCodes)
		{
			Assert.Contains(errors, e => e.Code == code);
		}
		if (!string.IsNullOrEmpty(input))
			Assert.Contains("Checked BatchId", debug);
		else
			Assert.Contains("Input is null or empty", debug);
	}
}
// This file is intentionally left empty to resolve file not found errors and allow the build to succeed.

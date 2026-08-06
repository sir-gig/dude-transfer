from dude_transfer.dataframe import diagnose_dataframe, plan_dataframe, send_dataframe


class FakeStats:
    def __init__(self, size):
        self.size = size

    def sizeInBytes(self):
        return self.size


class FakePlan:
    def __init__(self, size, text="Scan parquet"):
        self.size = size
        self.text = text

    def stats(self):
        return FakeStats(self.size)

    def toString(self):
        return self.text


class FakeQueryExecution:
    def __init__(self, plan):
        self.plan = plan

    def optimizedPlan(self):
        return self.plan


class FakeJdf:
    def __init__(self, plan):
        self.plan = plan

    def queryExecution(self):
        return FakeQueryExecution(self.plan)


class FakeRdd:
    def __init__(self, partitions):
        self.partitions = partitions

    def getNumPartitions(self):
        return self.partitions


class FakeWriter:
    def __init__(self):
        self.calls = []

    def format(self, value):
        self.calls.append(("format", value))
        return self

    def mode(self, value):
        self.calls.append(("mode", value))
        return self

    def option(self, key, value):
        self.calls.append(("option", key, value))
        return self

    def partitionBy(self, *columns):
        self.calls.append(("partitionBy", columns))
        return self

    def save(self, destination):
        self.calls.append(("save", destination))


class FakeDataFrame:
    def __init__(self, size, partitions, plan_text="Scan parquet"):
        self._jdf = FakeJdf(FakePlan(size, plan_text))
        self.rdd = FakeRdd(partitions)
        self.write = FakeWriter()
        self.repartitioned = None
        self.coalesced = None

    def repartition(self, count):
        self.repartitioned = count
        return self

    def coalesce(self, count):
        self.coalesced = count
        return self


def test_plan_repartitions_a_serialized_dataframe():
    df = FakeDataFrame(size=2 * 1024**3, partitions=1, plan_text="Repartition 1")
    plan = plan_dataframe(df, "s3://bucket/run", target_file_size="256MiB")
    assert plan.action == "repartition"
    assert plan.partitions_after == 8
    assert {item.code for item in plan.diagnostics} >= {"single_partition", "serialized_plan"}


def test_send_dataframe_uses_distributed_writer():
    df = FakeDataFrame(size=1024**3, partitions=1)
    result = send_dataframe(df, "s3://bucket/run", target_file_size="256MiB")
    assert df.repartitioned == 4
    assert ("save", "s3://bucket/run") in df.write.calls
    assert result.kind == "dataframe"


def test_diagnose_flags_one_partition():
    diagnostics = diagnose_dataframe(FakeDataFrame(size=100, partitions=1))
    assert any(item.code == "single_partition" for item in diagnostics)


def test_explicit_partition_count_is_honored():
    df = FakeDataFrame(size=1024**3, partitions=10)
    plan = plan_dataframe(df, "s3://bucket/run", partitions=8)
    assert plan.action == "repartition"
    assert plan.partitions_after == 8

#pragma once

#include "datashard_user_table.h"

#include <contrib/ydb/core/scheme/scheme_pathid.h>
#include <contrib/ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimrChangeExchange {
    class TChangeRecord;
}

namespace NKikimr::NDataShard {

class TChangeRecordBuilder;

class TChangeRecord {
    friend class TChangeRecordBuilder;

public:
    enum class ESource: ui8 {
        Unspecified = 0,
        InitialScan = 1,
    };

    enum class EKind: ui8 {
        AsyncIndex,
        CdcDataChange,
        CdcHeartbeat,
    };

public:
    ui64 GetOrder() const { return Order; }
    ui64 GetGroup() const { return Group; }
    ui64 GetStep() const { return Step; }
    ui64 GetTxId() const { return TxId; }
    ui64 GetLockId() const { return LockId; }
    ui64 GetLockOffset() const { return LockOffset; }
    const TPathId& GetPathId() const { return PathId; }
    EKind GetKind() const { return Kind; }
    const TString& GetBody() const { return Body; }
    ESource GetSource() const { return Source; }

    const TPathId& GetTableId() const { return TableId; }
    ui64 GetSchemaVersion() const { return SchemaVersion; }
    TUserTable::TCPtr GetSchema() const { return Schema; }

    void Serialize(NKikimrChangeExchange::TChangeRecord& record) const;

    TConstArrayRef<TCell> GetKey() const;
    i64 GetSeqNo() const;
    TString GetPartitionKey() const;
    TInstant GetApproximateCreationDateTime() const;
    bool IsBroadcast() const;

    TString ToString() const;
    void Out(IOutputStream& out) const;

private:
    ui64 Order = Max<ui64>();
    ui64 Group = 0;
    ui64 Step = 0;
    ui64 TxId = 0;
    ui64 LockId = 0;
    ui64 LockOffset = 0;
    TPathId PathId;
    EKind Kind;
    TString Body;
    ESource Source = ESource::Unspecified;

    ui64 SchemaVersion;
    TPathId TableId;
    TUserTable::TCPtr Schema;

    mutable TMaybe<TOwnedCellVec> Key;
    mutable TMaybe<TString> PartitionKey;

}; // TChangeRecord

class TChangeRecordBuilder {
    using EKind = TChangeRecord::EKind;
    using ESource = TChangeRecord::ESource;

public:
    explicit TChangeRecordBuilder(EKind kind) {
        Record.Kind = kind;
    }

    explicit TChangeRecordBuilder(TChangeRecord&& record)
        : Record(std::move(record))
    {
    }

    TChangeRecordBuilder& WithLockId(ui64 lockId) {
        Record.LockId = lockId;
        return *this;
    }

    TChangeRecordBuilder& WithLockOffset(ui64 lockOffset) {
        Record.LockOffset = lockOffset;
        return *this;
    }

    TChangeRecordBuilder& WithOrder(ui64 order) {
        Record.Order = order;
        return *this;
    }

    TChangeRecordBuilder& WithGroup(ui64 group) {
        Record.Group = group;
        return *this;
    }

    TChangeRecordBuilder& WithStep(ui64 step) {
        Record.Step = step;
        return *this;
    }

    TChangeRecordBuilder& WithTxId(ui64 txId) {
        Record.TxId = txId;
        return *this;
    }

    TChangeRecordBuilder& WithPathId(const TPathId& pathId) {
        Record.PathId = pathId;
        return *this;
    }

    TChangeRecordBuilder& WithTableId(const TPathId& tableId) {
        Record.TableId = tableId;
        return *this;
    }

    TChangeRecordBuilder& WithSchemaVersion(ui64 version) {
        Record.SchemaVersion = version;
        return *this;
    }

    TChangeRecordBuilder& WithSchema(TUserTable::TCPtr schema) {
        Record.Schema = schema;
        return *this;
    }

    TChangeRecordBuilder& WithBody(const TString& body) {
        Record.Body = body;
        return *this;
    }

    TChangeRecordBuilder& WithBody(TString&& body) {
        Record.Body = std::move(body);
        return *this;
    }

    TChangeRecordBuilder& WithSource(ESource source) {
        Record.Source = source;
        return *this;
    }

    TChangeRecord&& Build() {
        return std::move(Record);
    }

private:
    TChangeRecord Record;

}; // TChangeRecordBuilder

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NDataShard::TChangeRecord, out, value) {
    return value.Out(out);
}

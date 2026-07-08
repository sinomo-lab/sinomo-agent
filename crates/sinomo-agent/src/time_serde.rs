pub(crate) mod rfc3339 {
    use serde::de::Error as DeError;
    use serde::ser::Error as SerError;
    use serde::{Deserialize, Deserializer, Serializer};
    use time::format_description::well_known::Rfc3339;
    use time::{OffsetDateTime, UtcOffset};

    pub fn serialize<S>(value: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format_utc(*value).map_err(S::Error::custom)?)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        OffsetDateTime::parse(&value, &Rfc3339).map_err(D::Error::custom)
    }

    fn format_utc(value: OffsetDateTime) -> Result<String, &'static str> {
        let value = value.to_offset(UtcOffset::UTC);
        let date = value.date();
        let time = value.time();
        let year = date.year();
        if !(0..=9999).contains(&year) {
            return Err("timestamp year is outside RFC3339's four-digit range");
        }

        let mut output = format!(
            "{year:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            u8::from(date.month()),
            date.day(),
            time.hour(),
            time.minute(),
            time.second()
        );
        let nanos = time.nanosecond();
        if nanos != 0 {
            let mut fraction = format!("{nanos:09}");
            while fraction.ends_with('0') {
                fraction.pop();
            }
            output.push('.');
            output.push_str(&fraction);
        }
        output.push('Z');
        Ok(output)
    }

    pub(crate) mod option {
        use super::*;

        pub fn serialize<S>(
            value: &Option<OffsetDateTime>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match value {
                Some(value) => {
                    serializer.serialize_some(&format_utc(*value).map_err(S::Error::custom)?)
                }
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let value = Option::<String>::deserialize(deserializer)?;
            value
                .map(|value| OffsetDateTime::parse(&value, &Rfc3339).map_err(D::Error::custom))
                .transpose()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[derive(Debug, Deserialize, serde::Serialize)]
        struct TimestampFixture {
            #[serde(with = "crate::time_serde::rfc3339")]
            required: OffsetDateTime,
            #[serde(with = "crate::time_serde::rfc3339::option")]
            optional: Option<OffsetDateTime>,
        }

        #[test]
        fn serializes_utc_rfc3339() {
            let timestamp = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp")
                + time::Duration::nanoseconds(123_450_000);

            let value = serde_json::to_value(TimestampFixture {
                required: timestamp,
                optional: Some(timestamp),
            })
            .expect("serialize");

            assert_eq!(value["required"], "2023-11-14T22:13:20.12345Z");
            assert_eq!(value["optional"], "2023-11-14T22:13:20.12345Z");
        }

        #[test]
        fn deserializes_rfc3339_offset() {
            let value: TimestampFixture = serde_json::from_value(serde_json::json!({
                "required": "2023-11-14T17:13:20-05:00",
                "optional": null,
            }))
            .expect("deserialize");

            assert_eq!(
                value.required.to_offset(UtcOffset::UTC),
                OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp")
            );
            assert_eq!(value.optional, None);
        }
    }
}

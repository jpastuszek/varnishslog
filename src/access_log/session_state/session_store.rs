use store::VslStore;
use vsl::record::{
    VslIdent,
    VslRecord,
    VslRecordParseError,
};
use store::Config as StoreConfig;
use access_log::record::SessionRecord;
use access_log::record::Link;
use std::collections::HashMap;
use std::num::Wrapping;

use vsl::record::VslRecordTag::*;
use vsl::record::message::parser::*;

#[derive(Debug)]
enum Slot {
    Session(SessionRecord),
    Tombstone(VslRecordParseError),
}

#[derive(Debug)]
enum SlotAction {
    Updated,
    NotFound,
    Kill(VslRecordParseError),
}

#[derive(Debug)]
pub enum ApplyResult {
    Updated,
    NotFound,
}

#[derive(Debug)]
pub struct SessionStore {
    sessions: VslStore<Slot>,
    // lookup from req to sess
    reverse_lookup: HashMap<VslIdent, VslIdent>,
}

impl SessionStore {
    pub fn new() -> SessionStore {
        SessionStore::with_config(&Default::default())
    }

    pub fn with_config(store_config: &StoreConfig) -> SessionStore {
        SessionStore {
            sessions: VslStore::with_config("sessions", Some(Self::on_expire), Some(Self::on_expire), store_config),
            reverse_lookup: HashMap::new(),
        }
    }

    fn on_expire(store_name: &str, current_epoch: Wrapping<u64>, record_epoch: Wrapping<u64>, record_ident: VslIdent, record: &Slot) -> () {
        //TODO remove reverse_lookups for deleted session
    }

    pub fn insert(&mut self, session: SessionRecord) {
        for client_record in &session.client_records {
            if let Link::Unresolved(ident, _) = client_record {
                self.reverse_lookup.insert(*ident, session.ident); //TODO: check we are not owerriding
            } else {
                //TODO: dont make them links
                panic!("resolved session link?!?!");
            }
        }

        self.sessions.insert(session.ident, Slot::Session(session));
    }

    pub fn apply(&mut self, vsl: &VslRecord) -> ApplyResult {
        fn update_session(session: &mut SessionRecord, reverse_lookup: &mut HashMap<VslIdent, VslIdent>, vsl: &VslRecord) -> Result<(), VslRecordParseError> {
            match vsl.tag {
                SLT_Link => {
                    let (reason, child_ident, child_type) = try!(vsl.parse_data(slt_link));

                    match (reason, child_type) {
                        ("req", "rxreq") => {
                            session.client_records.push(Link::Unresolved(child_ident, child_type.to_owned()));
                            reverse_lookup.insert(child_ident, session.ident); // TODO: warn if already have the entry
                        }
                        _ => warn!("Ignoring unmatched SLT_Link in for session {}: reason variant: {}", session.ident, reason)
                    };
                }
                // TODO: remove on SessionClose + End
                _ => debug!("Ignoring unmatched VSL tag for session {}: {:?}", session.ident, vsl.tag)
            }
            Ok(())
        }

        let action = match self.sessions.get_mut(&vsl.ident) {
            None => SlotAction::NotFound,
            Some(Slot::Session(ref mut session)) => {
                match update_session(session, &mut self.reverse_lookup, vsl) {
                    Ok(()) => SlotAction::Updated,
                    Err(err) => SlotAction::Kill(err),
                }
            }
            Some(Slot::Tombstone(_err)) => SlotAction::NotFound, // TOOD: Log?
        };

        match action {
            SlotAction::NotFound => ApplyResult::NotFound,
            SlotAction::Updated => ApplyResult::Updated,
            SlotAction::Kill(err) => {
                self.sessions.insert(vsl.ident, Slot::Tombstone(err));
                //TODO: remove reverse
                ApplyResult::Updated
            }
        }
    }

    pub fn reverse_lookup(&self, ident: &VslIdent) -> Option<&SessionRecord> {
        self.reverse_lookup.get(ident).and_then(|session_ident|
            match self.sessions.get(session_ident).expect("reverse lookup found but session gone!") {
                Slot::Session(session) => Some(session),
                Slot::Tombstone(err) => None, //TODO: log
            }
        )
    }
}
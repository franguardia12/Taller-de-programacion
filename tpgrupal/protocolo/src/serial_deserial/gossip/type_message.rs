#[derive(PartialEq)]
pub enum TypeGossip {
    Syn,
    Ack,
    Ack2,
}

impl TypeGossip {
    pub fn create(n: u8) -> Self {
        match n {
            0x00 => TypeGossip::Syn,
            0x01 => TypeGossip::Ack,
            _ => TypeGossip::Ack2,
        }
    }

    pub fn valor(&self) -> u8 {
        match self {
            TypeGossip::Syn => 0x00,
            TypeGossip::Ack => 0x01,
            TypeGossip::Ack2 => 0x02,
        }
    }
}

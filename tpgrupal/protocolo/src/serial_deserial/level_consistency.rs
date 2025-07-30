#[derive(Debug, PartialEq, Clone)]
pub enum LevelConsistency {
    Strong,
    Weak,
}

impl LevelConsistency {
    pub fn create(n: u16) -> Self {
        match n {
            0x0004 => LevelConsistency::Strong,
            _ => LevelConsistency::Weak,
        }
    }

    pub fn valor(&self) -> u16 {
        match self {
            LevelConsistency::Strong => 0x0004, //QUORUM
            LevelConsistency::Weak => 0x0001,   //ONE
        }
    }
}

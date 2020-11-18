use num_traits::{Num, Zero, Signed, One};
use num_bigint::BigInt;
use std::ops::{Add, Mul, Rem, Div, Sub};
use std::cmp::Ordering;

pub const DEFAULT_BITS: u16 = 32;

#[derive(Debug, Clone)]
pub struct Integer {
    bits: u16,
    backing: BigInt
}


impl Integer {
    pub fn new(bits: u16, backing: BigInt) -> Self {
        Integer { bits, backing }
    }

    pub fn to_bits(&self, new_bits: u16) -> Self {
        if new_bits <= self.bits {
            self.clone()
        } else {
            let mut mask = BigInt::zero();
            for _ in 0..new_bits {
                mask = mask << 1;
                mask |= BigInt::one();
            }

            let masked = &self.backing & &mask;
            if masked == self.backing {
                Self {
                    bits: new_bits,
                    backing: masked
                }
            } else if self.backing.is_positive() {
                Self {
                    bits: new_bits,
                    backing: mask
                }
            } else if self.backing.is_negative() {
                Self {
                    bits: new_bits,
                    backing: -mask
                }
            } else {
                Self {
                    bits: new_bits,
                    backing: masked
                }
            }


        }
    }
}

impl Add for Integer {
    type Output = Integer;

    fn add(self, rhs: Self) -> Self::Output {
        let max_bits = if self.bits > rhs.bits {
            self.bits
        } else {
            rhs.bits
        };
        let backing = self.backing + rhs.backing;
        Integer::new(max_bits, backing).to_bits(max_bits)
    }
}

impl Sub for Integer {
    type Output = Integer;

    fn sub(self, rhs: Self) -> Self::Output {
        let max_bits = if self.bits > rhs.bits {
            self.bits
        } else {
            rhs.bits
        };
        let backing = self.backing - rhs.backing;
        Integer::new(max_bits, backing).to_bits(max_bits)
    }
}

impl Mul for Integer {
    type Output = Integer;

    fn mul(self, rhs: Self) -> Self::Output {
        let max_bits = if self.bits > rhs.bits {
            self.bits
        } else {
            rhs.bits
        };
        let backing = self.backing * rhs.backing;
        Integer::new(max_bits, backing).to_bits(max_bits)

    }
}

impl Rem for Integer {
    type Output = Integer;

    fn rem(self, rhs: Self) -> Self::Output {
        let max_bits = if self.bits > rhs.bits {
            self.bits
        } else {
            rhs.bits
        };
        let backing = self.backing % rhs.backing;
        Integer::new(max_bits, backing).to_bits(max_bits)

    }
}

impl Div for Integer {
    type Output = Integer;

    fn div(self, rhs: Self) -> Self::Output {
        let max_bits = if self.bits > rhs.bits {
            self.bits
        } else {
            rhs.bits
        };
        let backing = self.backing / rhs.backing;
        Integer::new(max_bits, backing).to_bits(max_bits)

    }
}

impl PartialEq for Integer {
    fn eq(&self, other: &Self) -> bool {
        self.backing == other.backing
    }
}

impl Eq for Integer { }

impl PartialOrd for Integer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl Ord for Integer {
    fn cmp(&self, other: &Self) -> Ordering {
        self.backing.cmp(&other.backing)
    }
}

impl Zero for Integer {
    fn zero() -> Self {
        Self {
            bits: DEFAULT_BITS,
            backing: Default::default()
        }
    }

    fn is_zero(&self) -> bool {
        self.backing.is_zero()
    }
}

impl One for Integer {
    fn one() -> Self {
        Self {
            bits: DEFAULT_BITS,
            backing: One::one()
        }
    }
}

impl Num for Integer {
    type FromStrRadixErr = <BigInt as Num>::FromStrRadixErr;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        let backing = BigInt::from_str_radix(str, radix)?;
        let size = backing.bits() as u16;
        Ok(Self {
            bits: size,
            backing
        })
    }
}




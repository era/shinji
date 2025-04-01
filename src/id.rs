#[derive(PartialEq, Debug, Clone)]
pub struct Id(pub [u8; 20]);

impl Id {
    pub fn distance(&self, other: &Id) -> Vec<u8> {
        self.0
            .iter()
            .zip(other.0.iter())
            .map(|(b1, b2)| b1 ^ b2)
            .collect()
    }

    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_identical_ids() {
        let id1 = Id([0; 20]);
        let id2 = Id([0; 20]);
        let expected_distance = vec![0; 20];
        assert_eq!(id1.distance(&id2), expected_distance);
    }

    #[test]
    fn test_distance_different_ids() {
        let id1 = Id([
            0b11001100, 0b10101010, 0b11110000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]);
        let id2 = Id([
            0b10101010, 0b11001100, 0b00001111, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]);
        let expected_distance = vec![
            0b01100110, // 11001100 ^ 10101010
            0b01100110, // 10101010 ^ 11001100
            0b11111111, // 11110000 ^ 00001111
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert_eq!(id1.distance(&id2), expected_distance);
    }

    #[test]
    fn test_distance_with_zero_bytes() {
        let id1 = Id([0; 20]);
        let id2 = Id([0b11111111; 20]); // All bytes set to 255
        let expected_distance = vec![0b11111111; 20]; // XOR with zero gives the same value
        assert_eq!(id1.distance(&id2), expected_distance);
    }
}

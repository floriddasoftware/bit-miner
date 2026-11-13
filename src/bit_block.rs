
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};

use bitcoin::{
    address::Address,
    base58,
    bip32::{
        ChainCode,
        ChildNumber,
        DerivationPath,
        Fingerprint,
        Xpriv,
        Xpub,
    },
    hashes::Hash,
    key::UntweakedPublicKey,
    secp256k1::{
        Keypair,
        PublicKey as SecpPublicKey,
        Secp256k1,
        SecretKey,
    },
    Address as BitcoinAddress,
    Network,
    NetworkKind,
    PublicKey,
    ScriptBuf,
    WPubkeyHash,
    Witness,
};

type BitBlockResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const ZPUB_VERSION: [u8; 4] = [0x04, 0xB2, 0x47, 0x46];
const MAX_BIP32_DEPTH: u64 = u8::MAX as u64;

fn invalid_input(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(IoError::new(ErrorKind::InvalidInput, message.into()))
}

fn activation_depth(activation: u64) -> BitBlockResult<u8> {
    u8::try_from(activation).map_err(|_| {
        invalid_input(format!(
            "activation exceeds extended-key depth limit: activation={} maximum={}",
            activation,
            MAX_BIP32_DEPTH,
        ))
    })
}

/// BIP32 path depth is the number of child components, excluding `m`.
///
/// m                  = 0
/// m/1                = 1
/// m/1/2              = 2
/// m/1/2/3            = 3
/// m/44'/0'/0'        = 3
/// m/44'/0'/0'/0/0    = 5
pub fn bip32_path_depth(path: &DerivationPath) -> usize {
    path.as_ref().len()
}

/// QTM-derived depth is activation multiplied by the number of BIP32
/// derivation components in the selected path.
pub fn qtm_path_depth(
    activation: u64,
    path: &DerivationPath,
) -> BitBlockResult<u8> {
    let levels = bip32_path_depth(path) as u64;

    let depth = activation
        .checked_mul(levels)
        .ok_or_else(|| invalid_input("QTM-derived depth overflow"))?;

    u8::try_from(depth).map_err(|_| {
        invalid_input(format!(
            "QTM-derived depth exceeds extended-key depth limit: activation={} path_levels={} derived_depth={} maximum={}",
            activation,
            levels,
            depth,
            MAX_BIP32_DEPTH,
        ))
    })
}

fn secret_key(private_key: [u8; 32]) -> BitBlockResult<SecretKey> {
    Ok(SecretKey::from_slice(&private_key)?)
}

fn secp256k1() -> Secp256k1<bitcoin::secp256k1::All> {
    Secp256k1::new()
}

fn compressed_public_key(
    private_key: [u8; 32],
) -> BitBlockResult<PublicKey> {
    let secp = secp256k1();
    let secret = secret_key(private_key)?;
    let public = SecpPublicKey::from_secret_key(&secp, &secret);

    Ok(PublicKey::new(public))
}

fn public_key_hash(
    private_key: [u8; 32],
) -> BitBlockResult<WPubkeyHash> {
    let public_key = compressed_public_key(private_key)?;
    Ok(WPubkeyHash::hash(&public_key.to_bytes()))
}

/// Construct the QTM extended private key directly.
///
/// coordinate = QTM private-key material
/// commitment = QTM BIP32 chain code
/// activation = QTM transit count
///
/// This is the un-navigated QTM key. Its initial depth records activation.
/// Once a concrete network path is supplied, the derived key depth is set
/// to activation multiplied by that path's BIP32 component count.
///
/// No BIP32 seed HMAC derivation is performed.
pub fn qtm_xpriv(
    coordinate: [u8; 32],
    commitment: [u8; 32],
    activation: u64,
    network: NetworkKind,
) -> BitBlockResult<Xpriv> {
    let private_key = secret_key(coordinate)?;
    let depth = activation_depth(activation)?;

    Ok(Xpriv {
        network,
        depth,
        parent_fingerprint: Fingerprint::default(),
        child_number: ChildNumber::Normal { index: 0 },
        private_key,
        chain_code: ChainCode::from(commitment),
    })
}

/// Derive an Xpriv along a concrete path and apply QTM's serialized depth.
///
/// The key material is derived by normal BIP32 child derivation.
/// The final serialized depth is QTM-specific:
///
///     activation * path_component_count
fn derive_qtm_xpriv(
    coordinate: [u8; 32],
    commitment: [u8; 32],
    activation: u64,
    path: &DerivationPath,
    network: NetworkKind,
) -> BitBlockResult<Xpriv> {
    let mut master = qtm_xpriv(
        coordinate,
        commitment,
        activation,
        network,
    )?;

    let depth = qtm_path_depth(activation, path)?;

    let secp = secp256k1();
    let mut child = master.derive_priv(&secp, path)?;

    child.depth = depth;

    Ok(child)
}

/// Convert an Xpub serialization to SLIP-0132 zpub format.
///
/// Only the four-byte extended-public-key version is replaced.
pub fn xpub_to_zpub(xpub: &Xpub) -> String {
    let mut payload = xpub.encode();
    payload[..4].copy_from_slice(&ZPUB_VERSION);

    base58::encode_check(&payload)
}

/// Return the QTM master fingerprint.
pub fn qtm_master_fingerprint(
    coordinate: [u8; 32],
    commitment: [u8; 32],
    activation: u64,
    network: NetworkKind,
) -> BitBlockResult<Fingerprint> {
    let master = qtm_xpriv(
        coordinate,
        commitment,
        activation,
        network,
    )?;

    Ok(master.fingerprint())
}

/// Return the QTM master Xpub.
pub fn qtm_master_xpub(
    coordinate: [u8; 32],
    commitment: [u8; 32],
    activation: u64,
    network: NetworkKind,
) -> BitBlockResult<Xpub> {
    let secp = secp256k1();

    let master = qtm_xpriv(
        coordinate,
        commitment,
        activation,
        network,
    )?;

    Ok(Xpub::from_priv(&secp, &master))
}

/// Return the QTM master Zpub serialization.
pub fn qtm_master_zpub(
    coordinate: [u8; 32],
    commitment: [u8; 32],
    activation: u64,
    network: NetworkKind,
) -> BitBlockResult<String> {
    let xpub = qtm_master_xpub(
        coordinate,
        commitment,
        activation,
        network,
    )?;

    Ok(xpub_to_zpub(&xpub))
}

/// Queryable QTM master and account-level extended-key information.
#[derive(Debug, Clone)]
pub struct QtmMasterKeys {
    pub activation: u64,
    pub depth: u8,
    pub fingerprint: String,
    pub xpriv: String,
    pub xpub: String,
    pub zpub: String,
    pub bip44_path: String,
    pub bip44_path_levels: usize,
    pub bip44_account_depth: u8,
    pub bip44_account_xpub: String,
    pub bip84_path: String,
    pub bip84_path_levels: usize,
    pub bip84_account_depth: u8,
    pub bip84_account_xpub: String,
    pub bip84_account_zpub: String,
}

/// Build QTM master information and BIP44/BIP84 account public keys.
///
/// The account paths shown here are defaults. Callers can supply the
/// network registry's concrete path to derive_qtm_child or the address
/// helpers below.
pub fn qtm_master_keys(
    coordinate: [u8; 32],
    commitment: [u8; 32],
    activation: u64,
    network: NetworkKind,
) -> BitBlockResult<QtmMasterKeys> {
    let secp = secp256k1();

    let master = qtm_xpriv(
        coordinate,
        commitment,
        activation,
        network,
    )?;

    let master_xpub = Xpub::from_priv(&secp, &master);

    let bip44_path: DerivationPath = "m/44'/0'/0'".parse()?;
    let bip84_path: DerivationPath = "m/84'/0'/0'".parse()?;

    let bip44_account = derive_qtm_xpriv(
        coordinate,
        commitment,
        activation,
        &bip44_path,
        network,
    )?;

    let bip84_account = derive_qtm_xpriv(
        coordinate,
        commitment,
        activation,
        &bip84_path,
        network,
    )?;

    let bip44_account_xpub =
        Xpub::from_priv(&secp, &bip44_account);

    let bip84_account_xpub =
        Xpub::from_priv(&secp, &bip84_account);

    Ok(QtmMasterKeys {
        activation,
        depth: master.depth,
        fingerprint: master.fingerprint().to_string(),
        xpriv: master.to_string(),
        xpub: master_xpub.to_string(),
        zpub: xpub_to_zpub(&master_xpub),
        bip44_path: bip44_path.to_string(),
        bip44_path_levels: bip32_path_depth(&bip44_path),
        bip44_account_depth: bip44_account.depth,
        bip44_account_xpub: bip44_account_xpub.to_string(),
        bip84_path: bip84_path.to_string(),
        bip84_path_levels: bip32_path_depth(&bip84_path),
        bip84_account_depth: bip84_account.depth,
        bip84_account_xpub: bip84_account_xpub.to_string(),
        bip84_account_zpub: xpub_to_zpub(&bip84_account_xpub),
    })
}

/// Derive a BIP32 child private key from the QTM coordinate/commitment pair.
///
/// `path` should be supplied by the selected network path registry.
///
/// The final serialized depth is:
///
///     activation * number_of_path_components
pub fn derive_qtm_child(
    coordinate: [u8; 32],
    commitment: [u8; 32],
    activation: u64,
    path: &str,
    network: NetworkKind,
) -> BitBlockResult<[u8; 32]> {
    let path: DerivationPath = path.parse()?;

    let child = derive_qtm_xpriv(
        coordinate,
        commitment,
        activation,
        &path,
        network,
    )?;

    Ok(child.private_key.secret_bytes())
}

/// Derive the compressed Bitcoin public key and its HASH160.
pub fn derive_public_key(
    private_key: [u8; 32],
) -> BitBlockResult<(PublicKey, WPubkeyHash)> {
    let public_key = compressed_public_key(private_key)?;
    let hash160 = WPubkeyHash::hash(&public_key.to_bytes());

    Ok((public_key, hash160))
}

/// Legacy P2PKH locking script.
pub fn p2pkh_script(
    private_key: [u8; 32],
) -> BitBlockResult<ScriptBuf> {
    let public_key = compressed_public_key(private_key)?;

    Ok(ScriptBuf::new_p2pkh(
        &public_key.pubkey_hash(),
    ))
}

/// Native SegWit P2WPKH locking script.
pub fn p2wpkh_script(
    private_key: [u8; 32],
) -> BitBlockResult<ScriptBuf> {
    let hash160 = public_key_hash(private_key)?;

    Ok(ScriptBuf::new_p2wpkh(&hash160))
}

/// Taproot P2TR key-path locking script.
pub fn taproot_script(
    private_key: [u8; 32],
) -> BitBlockResult<ScriptBuf> {
    let secp = secp256k1();
    let secret = secret_key(private_key)?;
    let keypair = Keypair::from_secret_key(&secp, &secret);
    let (x_only_public_key, _) = keypair.x_only_public_key();

    let internal_key: UntweakedPublicKey = x_only_public_key;

    Ok(ScriptBuf::new_p2tr(
        &secp,
        internal_key,
        None,
    ))
}

/// Default script derivation: Native SegWit P2WPKH.
pub fn derive_script_pubkey(
    private_key: [u8; 32],
) -> BitBlockResult<ScriptBuf> {
    p2wpkh_script(private_key)
}

/// Build an unsigned P2WPKH witness template.
///
/// This is not a valid spending witness until the transaction
/// signature has been generated.
pub fn derive_p2wpkh_witness(
    private_key: [u8; 32],
) -> BitBlockResult<Witness> {
    let public_key = compressed_public_key(private_key)?;

    let items: [Vec<u8>; 2] = [
        Vec::new(),
        public_key.to_bytes(),
    ];

    Ok(Witness::from_slice(&items))
}

/// Derive a Mainnet legacy P2PKH address.
pub fn p2pkh_address(
    private_key: [u8; 32],
) -> BitBlockResult<BitcoinAddress> {
    let script = p2pkh_script(private_key)?;

    Ok(Address::from_script(
        &script,
        Network::Bitcoin,
    )?)
}

/// Derive a Mainnet native SegWit P2WPKH address.
pub fn p2wpkh_address(
    private_key: [u8; 32],
) -> BitBlockResult<BitcoinAddress> {
    let script = p2wpkh_script(private_key)?;

    Ok(Address::from_script(
        &script,
        Network::Bitcoin,
    )?)
}

/// Derive a BIP44 legacy address using a registry-supplied path.
///
/// Default path: m/44'/0'/0'/0/0
pub fn qtm_bip44_address(
    coordinate: [u8; 32],
    commitment: [u8; 32],
    activation: u64,
    path: &str,
) -> BitBlockResult<BitcoinAddress> {
    let private_key = derive_qtm_child(
        coordinate,
        commitment,
        activation,
        path,
        NetworkKind::Main,
    )?;

    let script = p2pkh_script(private_key)?;

    Ok(Address::from_script(
        &script,
        Network::Bitcoin,
    )?)
}

/// Derive a BIP84 native SegWit address using a registry-supplied path.
///
/// Default path: m/84'/0'/0'/0/0
pub fn qtm_bip84_address(
    coordinate: [u8; 32],
    commitment: [u8; 32],
    activation: u64,
    path: &str,
) -> BitBlockResult<BitcoinAddress> {
    let private_key = derive_qtm_child(
        coordinate,
        commitment,
        activation,
        path,
        NetworkKind::Main,
    )?;

    let script = p2wpkh_script(private_key)?;

    Ok(Address::from_script(
        &script,
        Network::Bitcoin,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bip32_path_depth_counts_components_and_excludes_root() {
        let cases = [
            ("m", 0usize),
            ("m/1", 1),
            ("m/1/2", 2),
            ("m/1/2/3", 3),
            ("m/44'/0'/0'", 3),
            ("m/44'/0'/0'/0/0", 5),
        ];

        for (path, expected) in cases {
            let parsed: DerivationPath = path.parse().unwrap();

            assert_eq!(
                bip32_path_depth(&parsed),
                expected,
                "path={}",
                path,
            );
        }
    }

    #[test]
    fn qtm_depth_multiplies_activation_by_path_depth() {
        let cases = [
            (7u64, "m/1/2/3", 21u8),
            (7u64, "m/44'/0'/0'", 21),
            (7u64, "m/44'/0'/0'/0/0", 35),
            (1u64, "m/1/2/3", 3),
            (0u64, "m/1/2/3", 0),
        ];

        for (activation, path, expected) in cases {
            let parsed: DerivationPath = path.parse().unwrap();

            assert_eq!(
                qtm_path_depth(activation, &parsed).unwrap(),
                expected,
                "activation={} path={}",
                activation,
                path,
            );
        }
    }

    #[test]
    fn qtm_master_preserves_coordinate_commitment_and_activation() {
        let coordinate = [1u8; 32];
        let commitment = [2u8; 32];
        let activation = 7;

        let master = qtm_xpriv(
            coordinate,
            commitment,
            activation,
            NetworkKind::Main,
        )
        .unwrap();

        assert_eq!(
            master.private_key.secret_bytes(),
            coordinate,
        );

        assert_eq!(
            master.chain_code,
            ChainCode::from(commitment),
        );

        assert_eq!(master.depth, activation as u8);
    }

    #[test]
    fn account_depth_uses_activation_times_path_levels() {
        let coordinate = [1u8; 32];
        let commitment = [2u8; 32];
        let activation = 7;

        let keys = qtm_master_keys(
            coordinate,
            commitment,
            activation,
            NetworkKind::Main,
        )
        .unwrap();

        assert_eq!(keys.activation, activation);
        assert_eq!(keys.depth, 7);

        assert_eq!(keys.bip44_path_levels, 3);
        assert_eq!(keys.bip44_account_depth, 21);

        assert_eq!(keys.bip84_path_levels, 3);
        assert_eq!(keys.bip84_account_depth, 21);

        assert!(keys.xpriv.starts_with("xprv"));
        assert!(keys.xpub.starts_with("xpub"));
        assert!(keys.zpub.starts_with("zpub"));
        assert!(keys.bip44_account_xpub.starts_with("xpub"));
        assert!(keys.bip84_account_xpub.starts_with("xpub"));
        assert!(keys.bip84_account_zpub.starts_with("zpub"));
    }

    #[test]
    fn child_depth_uses_full_registry_path_level_count() {
        let coordinate = [1u8; 32];
        let commitment = [2u8; 32];
        let activation = 7;

        let path: DerivationPath =
            "m/84'/0'/0'/0/0".parse().unwrap();

        let child = derive_qtm_xpriv(
            coordinate,
            commitment,
            activation,
            &path,
            NetworkKind::Main,
        )
        .unwrap();

        assert_eq!(bip32_path_depth(&path), 5);
        assert_eq!(child.depth, 35);
    }

    #[test]
    fn bip44_and_bip84_paths_derive_distinct_children() {
        let coordinate = [1u8; 32];
        let commitment = [2u8; 32];
        let activation = 7;

        let legacy = derive_qtm_child(
            coordinate,
            commitment,
            activation,
            "m/44'/0'/0'/0/0",
            NetworkKind::Main,
        )
        .unwrap();

        let segwit = derive_qtm_child(
            coordinate,
            commitment,
            activation,
            "m/84'/0'/0'/0/0",
            NetworkKind::Main,
        )
        .unwrap();

        assert_ne!(legacy, segwit);
    }

    #[test]
    fn qtm_depth_cannot_exceed_extended_key_depth_byte() {
        let coordinate = [1u8; 32];
        let commitment = [2u8; 32];

        let result = derive_qtm_child(
            coordinate,
            commitment,
            52,
            "m/84'/0'/0'/0/0",
            NetworkKind::Main,
        );

        assert!(result.is_err());
    }

    #[test]
    fn activation_must_fit_initial_extended_key_depth() {
        let result = qtm_xpriv(
            [1u8; 32],
            [2u8; 32],
            256,
            NetworkKind::Main,
        );

        assert!(result.is_err());
    }
}

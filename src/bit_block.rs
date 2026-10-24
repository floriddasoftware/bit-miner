
use std::error::Error;

use bitcoin::{
    address::Address,
    hashes::Hash,
    key::UntweakedPublicKey,
    secp256k1::{Keypair, PublicKey as SecpPublicKey, Secp256k1, SecretKey},
    Address as BitcoinAddress, Network, PublicKey, ScriptBuf, WPubkeyHash, Witness,
};

type BitBlockResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn secret_key(private_key: [u8; 32]) -> BitBlockResult<SecretKey> {
    Ok(SecretKey::from_slice(&private_key)?)
}

fn secp256k1() -> Secp256k1<bitcoin::secp256k1::All> {
    Secp256k1::new()
}

fn compressed_public_key(private_key: [u8; 32]) -> BitBlockResult<PublicKey> {
    let secp = secp256k1();
    let secret = secret_key(private_key)?;
    let public = SecpPublicKey::from_secret_key(&secp, &secret);

    Ok(PublicKey::new(public))
}

fn public_key_hash(private_key: [u8; 32]) -> BitBlockResult<WPubkeyHash> {
    let public_key = compressed_public_key(private_key)?;
    Ok(WPubkeyHash::hash(&public_key.to_bytes()))
}

/// Derive the compressed Bitcoin public key and its HASH160.
///
/// The returned tuple is (public_key, hash160).
pub fn derive_public_key(
    private_key: [u8; 32],
) -> BitBlockResult<(PublicKey, WPubkeyHash)> {
    let public_key = compressed_public_key(private_key)?;
    let hash160 = WPubkeyHash::hash(&public_key.to_bytes());

    Ok((public_key, hash160))
}

/// Legacy P2PKH locking script.
///
/// Script form:
/// OP_DUP OP_HASH160 <20-byte public-key hash>
/// OP_EQUALVERIFY OP_CHECKSIG
pub fn p2pkh_script(private_key: [u8; 32]) -> BitBlockResult<ScriptBuf> {
    let public_key = compressed_public_key(private_key)?;
    Ok(ScriptBuf::new_p2pkh(&public_key.pubkey_hash()))
}

/// Native SegWit P2WPKH locking script.
///
/// Script form:
/// OP_0 <20-byte public-key hash>
pub fn p2wpkh_script(private_key: [u8; 32]) -> BitBlockResult<ScriptBuf> {
    let hash160 = public_key_hash(private_key)?;
    Ok(ScriptBuf::new_p2wpkh(&hash160))
}

/// Taproot P2TR key-path locking script.
///
/// Derives the x-only internal public key and applies the BIP341
/// TapTweak with no script-tree Merkle root.
pub fn taproot_script(private_key: [u8; 32]) -> BitBlockResult<ScriptBuf> {
    let secp = secp256k1();
    let secret = secret_key(private_key)?;
    let keypair = Keypair::from_secret_key(&secp, &secret);
    let (x_only_public_key, _) = keypair.x_only_public_key();

    let internal_key: UntweakedPublicKey = x_only_public_key;
    let script = ScriptBuf::new_p2tr(&secp, internal_key, None);

    Ok(script)
}

/// Default script derivation: Native SegWit P2WPKH.
///
/// Use p2pkh_script or taproot_script when a different output type
/// is explicitly required.
pub fn derive_script_pubkey(
    private_key: [u8; 32],
) -> BitBlockResult<ScriptBuf> {
    p2wpkh_script(private_key)
}

/// Build an unsigned P2WPKH witness template.
///
/// The stack contains an empty signature placeholder followed by
/// the compressed public key. This is NOT a valid spending witness
/// until the signature is generated for the transaction's sighash.
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

/// Derive the Mainnet native SegWit P2WPKH address.
pub fn p2wpkh_address(
    private_key: [u8; 32],
) -> BitBlockResult<BitcoinAddress> {
    let script = p2wpkh_script(private_key)?;

    Ok(Address::from_script(&script, Network::Bitcoin)?)
}

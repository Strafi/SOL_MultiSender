use std::{
    io::{self, BufRead},
    env,
    sync::Arc
};
use solana_sdk::{
    signer::{keypair::Keypair, Signer},
    instruction::Instruction,
    system_instruction::transfer,
    transaction::Transaction,
    message::Message,
};
use solana_client::nonblocking::rpc_client::RpcClient;


#[tokio::main]
async fn main()
 -> Result<(), anyhow::Error> {
    dotenv::dotenv().ok();

    let (rpc_url, main_wallet, threads): (String, Arc<Keypair>, usize) = (
        env::var("HTTP_RPC_URL")?,
        Arc::new(
            Keypair::from_base58_string(
            &env::var("MAIN_WALLET")?,
            )
        ),
        env::var("THREADS")?.parse()?
    );
    
    let semaphore = Arc::new(tokio::sync::Semaphore::new(threads));

    let sub_accounts: Vec<Arc<Keypair>> = tokio::fs::read_to_string("./files/accounts.txt")
        .await?
        .trim()
        .split("\n")
        .map(Keypair::from_base58_string)
        .map(Arc::new)
        .collect::<Vec<_>>();

    let client: Arc<RpcClient> = Arc::new(RpcClient::new(rpc_url));

    println!("Upload {} subaccounts\n", sub_accounts.len());

    println!("Menu\n1 - From main to subaccounts\n2 - From subaccounts to main (will withdraw all)");

    let mut buffer: String = String::new();
    io::stdin().lock().read_line(&mut buffer)?;

    let choice: u32 = buffer.trim().parse()?;

    match choice {
        1 => {
            println!("Enter the amount to transfer from main to subaccounts");
            let mut buffer: String = String::new();
            io::stdin().lock().read_line(&mut buffer)?;
        
            let withdraw_amount: f64 = buffer.trim().parse()?;
            let withdraw_amount_lamports: u64 = (withdraw_amount * 10u64.pow(9) as f64) as u64;
        
            // check if withdraw_amount*sub_accounts.len() > main_wallet balance
            let main_wallet_balance: u64 = client.get_balance(&main_wallet.pubkey()).await?;
            if withdraw_amount_lamports*sub_accounts.len() as u64 > main_wallet_balance {
                println!("Insufficient balance for all acounts\nPlease try again with a lower amount\n");
                return Ok(());
            }
                
            let main_wallet_clone = Arc::clone(&main_wallet);
            let client_clone = Arc::clone(&client);
        
            let handles = sub_accounts.iter().cloned().map(|sub_account| {
                let semaphore = Arc::clone(&semaphore);
                let main_wallet = Arc::clone(&main_wallet_clone);
                let client = Arc::clone(&client_clone);
                tokio::spawn(async move {
                    let permit = semaphore.acquire_owned().await;
        
					let mut counter = 0;
					while counter < 8 {
						counter += 1;
						tokio::time::sleep(std::time::Duration::from_secs(3)).await;
						let transfer_instr: Instruction = transfer(&main_wallet.pubkey(), &sub_account.pubkey(), withdraw_amount_lamports);
        
						let tx: Transaction = Transaction::new(
							&[&*main_wallet],
							Message::new(
								&[transfer_instr],
								Some(&main_wallet.pubkey()),
							),
							client.get_latest_blockhash().await.unwrap_or(client.get_latest_blockhash().await?)
						);
			
						let tx_result = client.send_and_confirm_transaction(&tx).await;
			
						match tx_result {
							Ok(tx_result) => {
								println!("Send tx with hash {}", tx_result);
								let success_subaccount = sub_account.to_base58_string();
								let file_content = tokio::fs::read_to_string("./files/success.txt").await?;
								let write_content = format!("{}\n{}", file_content, success_subaccount);
								tokio::fs::write("./files/success.txt", write_content).await?;
								println!("Success subaccount written to file");
								break;
							}
							Err(e) => {
								if e.to_string().contains("0x1") {
									println!("Error: This likely means there is insufficient balance\nFull err: {:?}", e);
								} else if e.to_string().contains("429") {
									println!("Error: This likely means too many requests\nFull err: {:?}", e);
									tokio::time::sleep(std::time::Duration::from_secs(3)).await;
								} else {
									println!("Error: {:?}", e);
								}
								println!("Retrying...");
							}
						}
					}
        
                    drop(permit);
        
                    Ok::<(), solana_client::client_error::ClientError>(())
                })
            });
        
            futures::future::join_all(handles).await;
        }
        2 => {
            let handles = sub_accounts.iter().cloned().map(|sub_account| {
                let semaphore = Arc::clone(&semaphore);
                tokio::spawn({
                    let main_wallet = main_wallet.clone();
                    let client = client.clone();
                    async move {
                        let permit = semaphore.acquire_owned().await;

						let mut counter = 0;
						while counter < 8 {
							counter += 1;
							tokio::time::sleep(std::time::Duration::from_secs(3)).await;
							let sub_acc_balance: u64 = client.get_balance(&sub_account.pubkey()).await?;
			
							if sub_acc_balance == 0 {
								println!("Account {} has no balance", sub_account.pubkey());
								return Ok::<(), solana_client::client_error::ClientError>(());
							}
			
							let transfer_instr: Instruction = transfer(&sub_account.pubkey(), &main_wallet.pubkey(), sub_acc_balance);
			
							let tx: Transaction = Transaction::new(
								&[&sub_account, &main_wallet],
								Message::new(
									&[transfer_instr],
									Some(&main_wallet.pubkey()),
								),
								client.get_latest_blockhash().await.unwrap_or(client.get_latest_blockhash().await?)
							);
			
							let tx_result = client.send_and_confirm_transaction(&tx).await;
			
							match tx_result {
								Ok(tx_result) => {
									println!("Send tx with hash {}", tx_result);
									let success_subaccount = sub_account.to_base58_string();
									let file_content = tokio::fs::read_to_string("./files/success.txt").await?;
									let write_content = format!("{}\n{}", file_content, success_subaccount);
									tokio::fs::write("./files/success.txt", write_content).await?;
									println!("Success subaccount written to file");
									break;
								}
								Err(e) => {
									if e.to_string().contains("0x1") {
										println!("Error: This likely means there is insufficient balance\nFull err: {:?}", e);
									} else if e.to_string().contains("429") {
										println!("Error: This likely means too many requests\nFull err: {:?}", e);
										tokio::time::sleep(std::time::Duration::from_secs(3)).await;
									} else {
										println!("Error: {}", e);
									}
									println!("Retrying...");
								}
							}
						}
                        
                        drop(permit);

                        Ok(())
                    }
                })
            });
        
            futures::future::join_all(handles).await;
        }
        _ => {
            println!("Invalid choice");
        }
    }


    Ok(())
}

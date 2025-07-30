use std::io::Write;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{io, thread};

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Job>,
    shutdown_sender: mpsc::Sender<()>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();
        let (shutdown_sender, shutdown_receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));
        let shutdown_receiver = Arc::new(Mutex::new(shutdown_receiver));

        let mut workers = Vec::with_capacity(size);

        for _id in 0..size {
            workers.push(Worker::new(
                Arc::clone(&receiver),
                Arc::clone(&shutdown_receiver),
            ));
        }

        ThreadPool {
            workers,
            sender,
            shutdown_sender,
        }
    }

    pub fn execute<F: FnOnce() + Send + 'static>(&self, f: F) {
        let job = Box::new(f);
        self.sender.send(job).unwrap();
    }

    pub fn shutdown(self) {
        println!("Finalizando simulador.");
        io::stdout().flush().unwrap();
        for _ in &self.workers {
            self.shutdown_sender.send(()).unwrap();
        }

        for worker in self.workers {
            if worker.thread.join().is_err() {
                println!("La base de datos se cerró con la consola funcionando");
                io::stdout().flush().unwrap();
            }
        }
    }
}

struct Worker {
    thread: thread::JoinHandle<()>,
}

impl Worker {
    fn new(
        receiver: Arc<Mutex<mpsc::Receiver<Job>>>,
        shutdown_receiver: Arc<Mutex<mpsc::Receiver<()>>>,
    ) -> Worker {
        let thread = thread::spawn(move || loop {
            let job = {
                let receiver = receiver.lock().unwrap();
                receiver.recv_timeout(Duration::from_secs(2)).ok()
            };
            match job {
                Some(job) => {
                    job();
                }
                None => {
                    let shutdown = {
                        let shutdown_receiver = shutdown_receiver.lock().unwrap();
                        shutdown_receiver.try_recv().ok()
                    };
                    if shutdown.is_some() {
                        break;
                    }
                }
            }
        });
        Worker { thread }
    }
}

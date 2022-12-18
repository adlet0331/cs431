use crossbeam_channel::{bounded, unbounded};
use cs431_homework::hello_server::{CancellableTcpListener, Handler, Statistics, ThreadPool};
use std::io;
use std::sync::Arc;

const ADDR: &str = "localhost:7878";

fn main() -> io::Result<()> {
    // Use a browser that doesn't cache too eagerly so that request is always sent. For example,
    // Firefox works well.  If you want to test using command line only, use curl. If you want to
    // run it on the lab server, you may need to change the port number to something else.
    println!(
        "Run `curl http://{}/KEY` to query the server with KEY",
        ADDR
    );

    // The thread pool.
    //
    // In the thread pool, we'll execute:
    //
    // - A listener: it accepts incoming connections, and creates a new worker for each connection.
    //      Connection에 반응하는 애들, Workers를 만들어준다.
    //
    // - Workers (once for each incoming connection): a worker handles an incoming connection and
    //   sends a corresponding report to the reporter.
    //      Connection 하나에 하나씩 만들어짐. 리포터에게 알맞은 반응을 보내준다.
    //
    // - A reporter: it aggregates the reports from the workers and processes the
    //   statistics.  When it ends, it sends the statistics to the main thread.
    //       리포트를 만든다. Worksers 로 부터 답변을 받고 뭔가 메인 쓰레드에 돌려준다.
    //
    let pool = Arc::new(ThreadPool::new(7));

    // The (MPSC) channel of reports between workers and the reporter.
    let (report_sender, report_receiver) = unbounded();

    // The (SPSC one-shot) channel of stats between the reporter and the main thread.
    let (stat_sender, stat_receiver) = bounded(0);

    // Listens to the address.
    let listener = Arc::new(CancellableTcpListener::bind(ADDR)?);

    // Installs a Ctrl-C handler.
    let ctrlc_listner_handle = listener.clone();
    ctrlc::set_handler(move || {
        ctrlc_listner_handle.cancel().unwrap();
    })
    .expect("Error setting Ctrl-C handler");

    // Executes the listener.
    let listener_pool = pool.clone();
    pool.execute(move || {
        // Creates the request handler.
        let handler = Handler::default();

        // For each incoming connection...
        for (id, stream) in listener.incoming().enumerate() {
            // send a job to the thread pool.
            let report_sender = report_sender.clone();
            let handler = handler.clone();
            listener_pool.execute(move || {
                let report = handler.handle_conn(id, stream.unwrap());
                report_sender.send(report).unwrap();
            });
        }
    });

    // Executes the reporter.
    pool.execute(move || {
        let mut stats = Statistics::default();
        for report in report_receiver {
            println!("[report] {report:?}");
            stats.add_report(report);
        }

        println!("[sending stat]");
        stat_sender.send(stats).unwrap();
        println!("[sent stat]");
    });

    // Blocks until the reporter sends the statistics.
    let stat = stat_receiver.recv().unwrap();
    println!("[stat] {stat:?}");

    Ok(())
    // When the pool is dropped, all worker threads are joined.
}

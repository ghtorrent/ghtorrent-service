require 'net/smtp'

module EmailStuff

  REQ_SUCCEEDED = <<-EMAIL
Subject: GHTorrent request
Date: %s

Dear %s,

Your request succedeed. Your request id is: %s.
You can view your request's status here:

  http://ghtorrent.org/lean%s

When your request has finished processing, you will receive a new email.

Thank you for using GHTorrent!
The GHTorrent team
  EMAIL

  DUMP_READY = <<-EMAIL
Subject: GHTorrent dump ready
Date: %s

Dear %s,

the data dumps for the data you requested can be downloaded from:

%s

The dump will be deleted after one week.

Enjoy!
The GHTorrent team
  EMAIL

  DUMP_FAILED = <<-EMAIL
Subject: GHTorrent backup failed
Date: %s

Dear %s,

unfortunately, your GHTorrent request could not be completed. We are
investigating the reason and you will be hearing from us soon.

The GHTorrent team
  EMAIL

  def send_req_succeed(email, name, id, url)
    text = sprintf(REQ_SUCCEEDED, Time.now, name, id, url)
    send_email(email, text)
  end

  def send_dump_succeed(email, name, url)
    text = sprintf(DUMP_READY, Time.now, name, url)
    send_email(email, text)
  end

  def send_dump_failed(email, name)
    text = sprintf(DUMP_READY, Time.now, name)
    send_email(email, text)
  end

  def send_email(to, text)
    Net::SMTP.start('localhost', 25, 'ghtorrent.org') do |smtp|
      begin
        smtp.send_message(text, 'GHTorrent Service<noreply@ghtorrent.org>', to)
      rescue Exception => e
        logger.error "Failed to send email to #{to}: #{e.message}"
        logger.error e.backtrace.join("\n")
      end
    end
  end

end

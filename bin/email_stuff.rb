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
  EMAIL

  BACKUP_READY = <<-EMAIL

  EMAIL


  def send_req_succeed(email, name, id, url)
    text = sprintf(REQ_SUCCEEDED, Time.now, name, id, url)
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

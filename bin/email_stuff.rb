module EmailStuff

  def send_email(to, text)
    Net::SMTP.start('localhost', 25, 'ghtorrent.org') do |smtp|
      begin
        smtp.send_message(text, 'noreply@ghtorrent.org', to)
      rescue
      end
    end
  end
end
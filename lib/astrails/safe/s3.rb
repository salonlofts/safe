module Astrails
  module Safe
    class S3 < Sink
      MAX_S3_FILE_SIZE = 5368709120

      def initialize(config, backup)
        super(config, backup)
        credentials = Aws::Credentials.new(key, secret)
        @connection = Aws::S3::Resource.new(:credentials => credentials, :region => region) unless local_only?
      end

      protected

      def active?
        bucket_name && key && secret
      end

      def path
        @path ||= expand(config[:s3, :path] || config[:local, :path] || ":kind/:id")
      end

      def save
        # FIXME: user friendly error here :)
        raise RuntimeError, "pipe-streaming not supported for S3." unless @backup.path

        puts "Uploading #{bucket_name}:#{full_path}" if verbose? || dry_run?
        unless dry_run? || local_only?
          if File.stat(@backup.path).size > MAX_S3_FILE_SIZE
            STDERR.puts "ERROR: File size exceeds maximum allowed for upload to S3 (#{MAX_S3_FILE_SIZE}): #{@backup.path}"
            return
          end
          benchmark = Benchmark.realtime do
            bucket = @connection.create_bucket(:bucket => bucket_name) #unless bucket_exists?(bucket_name)
            File.open(@backup.path) do |file|
              bucket.object(full_path).upload_file(file)
            end
          end
          puts "...done" if verbose?
          puts("Upload took " + sprintf("%.2f", benchmark) + " second(s).") if verbose?
        end
      end

      def cleanup
        return if local_only?

        return unless keep = config[:keep, :s3]

        puts "listing files: #{bucket_name}:#{base}" if verbose?
        files = @connection.bucket(bucket_name).objects(prefix: base)
        puts files.collect {|x| x.key} if verbose?

        files = files.
          collect {|x| x.key}.
          sort

        cleanup_with_limit(files, keep) do |f|
          puts "removing s3 file #{bucket_name}:#{f}" if dry_run? || verbose?
          @connection.bucket(bucket_name).object(f).delete unless dry_run? || local_only?
        end
      end

      def bucket_name
        config[:s3, :bucket_name]
      end

      def key
        config[:s3, :key]
      end

      def secret
        config[:s3, :secret]
      end

      def region
        config[:s3, :region]
      end

      private

      def bucket_exists?(bucket_name)
        @connection.bucket(bucket_name).exists?
      end
    end
  end
end

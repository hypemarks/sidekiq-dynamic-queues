require 'sidekiq-dynamic-queues'

module Sidekiq
  module DynamicQueues
    module Server

      Attr = Sidekiq::DynamicQueues::Attributes

      def self.registered(app)
        app.get "/dynamicqueue" do
          @queues = []
          dqueues = Attr.get_dynamic_queues
          dqueues.each do |k, v|
            expanded = Attr.expand_queues_for_display(["@#{k}"])
            view_data = {
                'name' => k,
                'value' => Array(v).join(", "),
                'expanded' => expanded.join(", ")
            }
            @queues << view_data
          end

          @queues.sort! do |a, b|
            an = a['name']
            bn = b['name']
            if an == 'default'
              1
            elsif bn == 'default'
              -1
            else
              an <=> bn
            end
          end

          view_path = File.join(File.expand_path("..", __FILE__), "server", "views")
          render(:erb, File.read(File.join(view_path, "dynamicqueue.erb")))
        end

        app.post "/dynamicqueue" do
          dynamic_queues = Array(params['queues'])
          queues = {}
          dynamic_queues.each do |queue|
            key = queue['name']
            values = queue['value'].to_s.split(',').collect{|q| q.gsub(/\s/, '') }
            queues[key] = values
          end
          Attr.set_dynamic_queues(queues)
          redirect "#{root_path}dynamicqueue"
        end

        app.tabs["DynamicQueues"] = "dynamicqueue"
      end
    end

  end
end

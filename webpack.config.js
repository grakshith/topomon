const path = require('path');
const webpack = require('webpack');

entry = {
  core: './src/client/index.ts'
}

// TODO: make the mode production 

module.exports = {
  entry: entry,
  mode: 'production',
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: {
          loader: 'ts-loader',
          options: {
            transpileOnly: true,
          }
        },
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: ['.tsx', '.ts', '.js'],
  },
  output: {
    filename: '[name].js',
    path: path.resolve(__dirname, 'dist'),
  },
  devServer:{
    contentBase: path.join(__dirname, 'dist'),
    port: 9000,
  },
};

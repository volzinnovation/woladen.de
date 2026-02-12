import SwiftUI
import UIKit

struct AmenityIconView: View {
    let amenityKey: String
    var size: CGFloat = 18

    var body: some View {
        Group {
            if let uiImage = loadImage(for: amenityKey) {
                Image(uiImage: uiImage)
                    .resizable()
                    .scaledToFit()
            } else {
                Image(systemName: "mappin.circle")
                    .resizable()
                    .scaledToFit()
                    .foregroundStyle(.secondary)
            }
        }
        .frame(width: size, height: size)
    }

    private func loadImage(for key: String) -> UIImage? {
        guard let filename = AmenityCatalog.iconFilename(for: key) else {
            return nil
        }

        let parts = filename.split(separator: ".", maxSplits: 1, omittingEmptySubsequences: false)
        let name = String(parts.first ?? Substring(filename))
        let ext = parts.count > 1 ? String(parts[1]) : nil

        if let inMain = UIImage(named: name) {
            return inMain
        }

        if let url = Bundle.main.url(forResource: name, withExtension: ext, subdirectory: "img"),
           let data = try? Data(contentsOf: url),
           let img = UIImage(data: data) {
            return img
        }
        return nil
    }
}
